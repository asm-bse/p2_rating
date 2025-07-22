import pandas as pd
import numpy as np
from datetime import datetime
from typing import Optional
from database.postgres_client import PostgreSQLClient


class RatingCalculator:
    """Класс для расчета рейтинга сотрудников"""
    
    def __init__(self, rating_date: Optional[datetime] = None):
        """
        Инициализация калькулятора рейтинга
        
        Args:
            rating_date: Дата на которую рассчитывается рейтинг. 
                        Если не указана, используется текущая дата
        """
        self.rating_date = rating_date or datetime.now()
        
        # Создаем экземпляр PostgreSQL клиента
        self.postgres_client = PostgreSQLClient()
    
    def calculate_stage_mark(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Рассчитывает оценку стажа для сотрудников
        
        Args:
            df: DataFrame с данными сотрудников, должен содержать колонку 'date_employment_employee'
            
        Returns:
            DataFrame с полными данными и добавленной колонкой 'stage_mark'
        """
        df_result = df.copy()
        
        # Преобразуем дату трудоустройства в datetime если нужно
        if 'date_employment_employee' in df_result.columns:
            df_result['date_employment_employee'] = pd.to_datetime(df_result['date_employment_employee'])
        
        def stage_mark_func(row):
            """
            Функция для определения оценки стажа:
            0 - испытательный срок, до 3 мес,
            1 - 3 мес до 6 мес
            2 - 6 мес. -1 года 
            3 - 1-3 лет
            4 - 3-5 лет
            5 - более 5 лет
            """
            employment_date = row['date_employment_employee']
            
            if pd.isna(employment_date):
                return 0
            elif employment_date >= self.rating_date - pd.DateOffset(months=3):
                return 0
            elif employment_date >= self.rating_date - pd.DateOffset(months=6):
                return 1
            elif employment_date >= self.rating_date - pd.DateOffset(years=1):
                return 2
            elif employment_date >= self.rating_date - pd.DateOffset(years=3):
                return 3
            elif employment_date >= self.rating_date - pd.DateOffset(years=5):
                return 4
            else:
                return 5
        
        # Добавляем расчет стажа в днях для удобства
        df_result['employment_days'] = (self.rating_date - df_result['date_employment_employee']).dt.days
        df_result['stage_mark'] = df_result.apply(stage_mark_func, axis=1)
        
        return df_result
    
    def calculate_performance_mark(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Рассчитывает оценку производительности сотрудников на основе данных из salary_archive
        
        Args:
            df: DataFrame с данными сотрудников, должен содержать колонку 'id_employee'
            
        Returns:
            DataFrame с полными данными и добавленными колонками производительности
        """
        df_result = df.copy()
        
        # Определяем даты для выборки данных
        rate_date = self.rating_date.date()
        date_to_reset = (pd.to_datetime(rate_date) - pd.DateOffset(months=1) + pd.DateOffset(days=2)).date()
        
        # Получаем данные из базы через PostgreSQL клиент
        df_salary_archive = self.postgres_client.read_table('public.salary_archive')
        
        # Преобразуем дату в datetime если нужно
        df_salary_archive['date'] = pd.to_datetime(df_salary_archive['date']).dt.date
        
        # Фильтруем по датам
        df_salary_archive = df_salary_archive.loc[(df_salary_archive['date'] >= date_to_reset)]
        df_salary_archive = df_salary_archive.loc[(df_salary_archive['date'] <= rate_date)]
        
        # Подсчитываем общее количество задач
        df_salary_archive_all = df_salary_archive.groupby(['id_employee'], as_index=False).count()
        df_salary_archive_all = df_salary_archive_all[['id_employee', 'coefficient']]
        df_salary_archive_all = df_salary_archive_all.rename(columns={'coefficient': 'all_tasks'})
        
        # Подсчитываем количество плохих задач (коэффициент < 0.99)
        df_salary_archive_bad = df_salary_archive.loc[(df_salary_archive['coefficient'] < 0.99)]
        df_salary_archive_bad = df_salary_archive_bad.groupby(['id_employee'], as_index=False).count()
        df_salary_archive_bad = df_salary_archive_bad[['id_employee', 'coefficient']]
        df_salary_archive_bad = df_salary_archive_bad.rename(columns={'coefficient': 'bad_tasks'})
        
        # Объединяем данные
        df_performance = df_salary_archive_all.merge(df_salary_archive_bad, how='left', on='id_employee')
        df_performance['bad_tasks'] = df_performance['bad_tasks'].fillna(0)
        df_performance['bad_tasks_share'] = df_performance['bad_tasks'] / df_performance['all_tasks']
        
        def performance_mark_func(row):
            """
            Функция для определения оценки производительности:
            25 - отличная (0% брака)
            15 - хорошая (< 4% брака)
            10 - удовлетворительная (< 7% брака)
            5 - неудовлетворительная (>= 7% брака)
            """
            bad_share = row['bad_tasks_share']
            
            if bad_share == 0:
                return 25
            elif bad_share < 0.04:
                return 15
            elif bad_share < 0.07:
                return 10
            elif bad_share >= 0.07:
                return 5
            else:
                return 0  # На случай непредвиденных ситуаций
        
        df_performance['performance_mark'] = df_performance.apply(performance_mark_func, axis=1)
        df_performance = df_performance.round(2)
        
        # Объединяем с основным DataFrame (полный merge со всеми данными)
        df_result = df_result.merge(df_performance, how='left', on='id_employee')
        
        # Заполняем NaN значения для сотрудников без данных о производительности
        df_result['performance_mark'] = df_result['performance_mark'].fillna(0)
        df_result['all_tasks'] = df_result['all_tasks'].fillna(0)
        df_result['bad_tasks'] = df_result['bad_tasks'].fillna(0)
        df_result['bad_tasks_share'] = df_result['bad_tasks_share'].fillna(0)
        
        return df_result
    
    def calculate_rating(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Рассчитывает общий рейтинг сотрудников
        
        Args:
            df: DataFrame с данными сотрудников
            
        Returns:
            DataFrame с рассчитанным рейтингом
        """
        df_base = df.copy()
        
        # Рассчитываем оценку стажа независимо
        df_stage = self.calculate_stage_mark(df_base)
        
        # Рассчитываем оценку производительности независимо
        df_performance = self.calculate_performance_mark(df_base)
        
        # Мержим результаты, оставляя только ключевые поля и оценки
        df_stage_clean = df_stage[['id_employee', 'stage_mark', 'employment_days']].copy()
        df_performance_clean = df_performance[['id_employee', 'performance_mark', 'all_tasks', 'bad_tasks', 'bad_tasks_share']].copy()
        
        # Объединяем базовые данные с оценками
        df_result = df_base.merge(df_stage_clean, on='id_employee', how='left')
        df_result = df_result.merge(df_performance_clean, on='id_employee', how='left')
        
        # Заполняем пропуски
        df_result['stage_mark'] = df_result['stage_mark'].fillna(0)
        df_result['performance_mark'] = df_result['performance_mark'].fillna(0)
        
        # Рассчитываем итоговый рейтинг
        df_result['total_rating'] = df_result['stage_mark'] + df_result['performance_mark']
        
        return df_result
    
    def get_linear_employees_rating(self, df_shtat: pd.DataFrame) -> pd.DataFrame:
        """
        Получает рейтинг для линейных сотрудников
        
        Args:
            df_shtat: DataFrame со штатом сотрудников
            
        Returns:
            DataFrame с рейтингом линейных сотрудников
        """
        # Фильтруем линейных сотрудников
        linear_positions = [
            'Тестировщик', 
            'Сборщик готовой продукции', 
            'Сборщик РЭА', 
            'Укладчик-упаковщик', 
            'Сборщик ', 
            'Маркировщик'
        ]
        
        df_linear = df_shtat[df_shtat['job_employee'].isin(linear_positions)].copy()
        
        # Рассчитываем рейтинг
        df_linear_rated = self.calculate_rating(df_linear)
        
        return df_linear_rated