import pandas as pd
import requests
import time
import os
from typing import List, Dict, Optional
from loguru import logger
from sqlalchemy import text

from base.mes_client import MESClient
from database.postgres_client import PostgreSQLClient


class EmployeeService(MESClient):
    """Сервис для работы с данными сотрудников"""
    
    def __init__(self):
        super().__init__()
        self.api_url = "https://apimes.starline.ru/v1/staterest"
        self.additional_data_api_url = "https://apimes.starline.ru/v1/work"
        self.db_client = PostgreSQLClient()
        self.request_delay = float(os.getenv('REQUEST_DELAY', 0.5))
        
    def get_all_employees_from_api(self, max_pages: int = 1000) -> List[Dict]:
        """Получение всех сотрудников из MES API с пагинацией"""
        logger.info("🔍 Начинаю получение всех сотрудников из MES API...")
        
        all_employees = []
        page = 1
        
        try:
            self.token = self.get_auth_token()
            
            while page <= max_pages:
                if page % 10 == 0 or page <= 10:
                    logger.info(f"📄 Обрабатываю страницу {page}...")
                
                try:
                    response = requests.get(
                        self.api_url,
                        params={'page': page},
                        headers={'Authorization': f'Bearer {self.token}'},
                        timeout=15
                    )
                    
                    if response.status_code == 200:
                        page_data = response.json()
                        
                        if len(page_data) == 0:
                            logger.info(f"📭 Страница {page} пустая. Завершаем сбор данных.")
                            break
                        
                        existing_ids = {emp['id_employee'] for emp in all_employees}
                        new_employees = [emp for emp in page_data if emp['id_employee'] not in existing_ids]
                        all_employees.extend(new_employees)
                        
                        logger.debug(f"Страница {page}: получено {len(page_data)} записей, новых: {len(new_employees)}")
                        
                        if len(new_employees) == 0:
                            logger.info(f"🔄 На странице {page} только дубликаты. Возможно, достигли конца.")
                            break
                            
                    elif response.status_code == 401:
                        logger.warning("🔑 Токен истек, получаю новый...")
                        self.token = self.get_auth_token()
                        continue
                        
                    else:
                        logger.error(f"❌ Ошибка на странице {page}: статус {response.status_code}")
                        break
                        
                except requests.RequestException as e:
                    logger.error(f"🌐 Ошибка при запросе страницы {page}: {e}")
                    if page == 1:
                        raise
                    break
                
                page += 1
                time.sleep(self.request_delay)
                
            logger.info(f"✅ Получено {len(all_employees)} уникальных сотрудников")
            return all_employees
            
        except Exception as e:
            logger.error(f"💥 Критическая ошибка при получении сотрудников: {e}")
            raise

    def get_additional_employees_data_from_api(self, max_pages: int = 1000) -> List[Dict]:
        """Получение дополнительных данных сотрудников из MES API с выбором самых свежих записей"""
        logger.info("🔍 Начинаю получение дополнительных данных сотрудников из MES API...")
        
        all_employee_records = []
        page = 1
        seen_page_hashes = set()
        
        try:
            self.token = self.get_auth_token()
            
            while page <= max_pages:
                if page % 10 == 0 or page <= 10:
                    logger.info(f"📄 Обрабатываю страницу {page}...")
                
                try:
                    response = requests.get(
                        self.additional_data_api_url,
                        params={'page': page},
                        headers={'Authorization': f'Bearer {self.token}'},
                        timeout=15
                    )
                    
                    if response.status_code == 200:
                        page_data = response.json()
                        
                        if len(page_data) == 0:
                            logger.info(f"📭 Страница {page} пустая. Завершаем сбор данных.")
                            break
                        
                        page_hash = self._get_page_hash(page_data)
                        
                        if page_hash in seen_page_hashes:
                            logger.warning(f"⚠️ Страница {page} является дубликатом (хеш: {page_hash[:8]}...)")
                            logger.info(f"🔄 Найден дубликат на странице {page}. Завершаем сбор данных.")
                            break
                        else:
                            seen_page_hashes.add(page_hash)
                            all_employee_records.extend(page_data)
                            logger.debug(f"Страница {page}: получено {len(page_data)} записей (уникальная)")
                            
                    elif response.status_code == 401:
                        logger.warning("🔑 Токен истек, получаю новый...")
                        self.token = self.get_auth_token()
                        continue
                        
                    else:
                        logger.error(f"❌ Ошибка на странице {page}: статус {response.status_code}")
                        break
                        
                except requests.RequestException as e:
                    logger.error(f"🌐 Ошибка при запросе страницы {page}: {e}")
                    if page == 1:
                        raise
                    break
                
                page += 1
                time.sleep(self.request_delay)
                
            logger.info(f"📥 Получено {len(all_employee_records)} записей всего с {len(seen_page_hashes)} уникальных страниц")
            
            latest_employee_data = self._filter_latest_employee_records(all_employee_records)
            
            logger.info(f"✅ После фильтрации получено {len(latest_employee_data)} уникальных сотрудников с самыми свежими данными")
            return latest_employee_data
            
        except Exception as e:
            logger.error(f"💥 Критическая ошибка при получении данных сотрудников: {e}")
            raise

    def _filter_latest_employee_records(self, all_records: List[Dict]) -> List[Dict]:
        """Фильтрует записи, оставляя только самые свежие для каждого сотрудника по date_work"""
        from datetime import datetime
        
        employee_latest_records = {}
        
        for record in all_records:
            emp_id = record.get('id_employee')
            date_work_str = record.get('date_work')
            
            if not emp_id or not date_work_str:
                logger.debug(f"Пропускаю запись без id_employee или date_work: {record}")
                continue
            
            try:
                if isinstance(date_work_str, str):
                    if date_work_str == '0000-00-00' or date_work_str.startswith('0000'):
                        date_work = datetime(1900, 1, 1)
                    else:
                        date_work = datetime.strptime(date_work_str[:10], '%Y-%m-%d')
                else:
                    date_work = date_work_str
                
                if (emp_id not in employee_latest_records or 
                    date_work > employee_latest_records[emp_id]['parsed_date']):
                    
                    employee_latest_records[emp_id] = {
                        'data': record,
                        'parsed_date': date_work
                    }
                    
            except (ValueError, TypeError) as e:
                logger.warning(f"Ошибка при парсинге даты '{date_work_str}' для сотрудника {emp_id}: {e}")
                if emp_id not in employee_latest_records:
                    employee_latest_records[emp_id] = {
                        'data': record,
                        'parsed_date': datetime(1900, 1, 1)
                    }
        
        latest_records = [emp_data['data'] for emp_data in employee_latest_records.values()]
        
        logger.info(f"🔍 Обработка дубликатов: из {len(all_records)} записей выбрано {len(latest_records)} уникальных")
        
        return latest_records

    def _get_page_hash(self, page_data: List[Dict]) -> str:
        """Создает хеш страницы для проверки уникальности"""
        import hashlib
        import json
        
        try:
            sorted_data = sorted(page_data, key=lambda x: str(x.get('id_employee', '')))
            hash_string = json.dumps(sorted_data, sort_keys=True, ensure_ascii=False)
            return hashlib.md5(hash_string.encode('utf-8')).hexdigest()
        
        except Exception as e:
            logger.debug(f"Ошибка при создании хеша страницы: {e}")
            ids = [str(item.get('id_employee', '')) for item in page_data[:5]]
            return f"len{len(page_data)}_ids{'_'.join(ids)}"

    def get_current_employees_with_work_data(self, from_api: bool = False) -> pd.DataFrame:
        """Получение действующих сотрудников с рабочими данными"""
        if from_api:
            return self._get_current_employees_from_api()
        else:
            return self._get_current_employees_from_db()

    def _get_current_employees_from_api(self) -> pd.DataFrame:
        """Получение действующих сотрудников с рабочими данными из API"""
        try:
            logger.info("🔍 Получаю действующих сотрудников с рабочими данными из API...")
            
            employees_data = self.get_all_employees_from_api()
            if not employees_data:
                logger.warning("⚠️ Нет данных о сотрудниках из API")
                return pd.DataFrame()
            
            df_employees = pd.DataFrame(employees_data)
            df_current = self._filter_current_employees(df_employees)
            
            if df_current.empty:
                return df_current
            
            work_data = self.get_additional_employees_data_from_api()
            if not work_data:
                logger.warning("⚠️ Нет рабочих данных из API")
                return df_current[['id_employee', 'fio_employee', 'date_employment_employee']]
            
            return self._merge_employee_work_data(df_current, pd.DataFrame(work_data))
            
        except Exception as e:
            logger.error(f"💥 Ошибка при получении действующих сотрудников из API: {e}")
            raise

    def _get_current_employees_from_db(self) -> pd.DataFrame:
        """Получение действующих сотрудников с рабочими данными из БД"""
        try:
            logger.info("🔍 Получаю действующих сотрудников с рабочими данными из БД...")
            
            df_employees = self.get_employees_from_postgres('mes_employees')
            if df_employees.empty:
                logger.warning("⚠️ Нет данных о сотрудниках в БД")
                return pd.DataFrame()
            
            df_current = self._filter_current_employees(df_employees)
            if df_current.empty:
                return df_current
            
            try:
                df_work = pd.read_sql('SELECT * FROM mes_employees_work', con=self.db_client.engine)
                return self._merge_employee_work_data(df_current, df_work)
                
            except Exception as work_error:
                logger.warning(f"⚠️ Ошибка при получении рабочих данных из БД: {work_error}")
                logger.info("📝 Возвращаю только основные данные сотрудников")
                return df_current[['id_employee', 'fio_employee', 'date_employment_employee']]
            
        except Exception as e:
            logger.error(f"💥 Ошибка при получении действующих сотрудников из БД: {e}")
            raise

    def _filter_current_employees(self, df_employees: pd.DataFrame) -> pd.DataFrame:
        """Фильтрует действующих сотрудников с 5-значными ID"""
        df_current = df_employees[
            (df_employees['id_employee'].astype(str).str.len() == 5) &
            (df_employees['date_dismissal_employee'].isna() | 
             (df_employees['date_dismissal_employee'] == '') |
             (df_employees['date_dismissal_employee'] == '0000-00-00'))
        ].copy()
        
        if df_current.empty:
            logger.warning("⚠️ Не найдено действующих сотрудников с 5-значными ID")
        else:
            logger.info(f"📊 Найдено {len(df_current)} действующих сотрудников")
        
        return df_current

    def _merge_employee_work_data(self, df_current: pd.DataFrame, df_work: pd.DataFrame) -> pd.DataFrame:
        """Объединяет данные сотрудников с рабочими данными"""
        df_result = df_current.merge(
            df_work[['id_employee', 'job_employee', 'subdivision_employee', 
                    'department_employee', 'area_employee']],
            on='id_employee',
            how='left'
        )
        
        columns_to_keep = [
            'id_employee', 'fio_employee', 'date_employment_employee',
            'job_employee', 'subdivision_employee', 'department_employee', 'area_employee'
        ]
        
        df_result = df_result[columns_to_keep]
        
        employees_with_work_data = df_result[df_result['job_employee'].notna()]
        employees_without_work_data = df_result[df_result['job_employee'].isna()]
        
        logger.info(f"   С рабочими данными: {len(employees_with_work_data)}")
        logger.info(f"   Без рабочих данных: {len(employees_without_work_data)}")
        
        return df_result

    def export_current_employees(self, filename: str = None, from_api: bool = False) -> str:
        """Экспорт действующих сотрудников с рабочими данными в CSV"""
        try:
            logger.info("🔍 Начинаю экспорт действующих сотрудников...")
            
            df_current_employees = self.get_current_employees_with_work_data(from_api=from_api)
            
            if df_current_employees.empty:
                logger.warning("❌ Нет данных о действующих сотрудниках для экспорта")
                return ""
            
            if filename is None:
                from datetime import datetime
                timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M')
                filename = f"exports/current_employees_{timestamp}.csv"
            
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            df_current_employees.to_csv(filename, index=False, encoding='utf-8-sig')
            
            logger.success(f"✅ Экспорт действующих сотрудников завершен: {filename}")
            logger.info(f"📊 Всего действующих сотрудников: {len(df_current_employees)}")
            
            self._log_statistics(df_current_employees)
            
            return filename
            
        except Exception as e:
            logger.error(f"❌ Ошибка при экспорте действующих сотрудников: {e}")
            raise

    def _log_statistics(self, df: pd.DataFrame):
        """Логирование статистики по DataFrame"""
        if 'subdivision_employee' in df.columns:
            subdivision_stats = df['subdivision_employee'].value_counts()
            logger.info("📈 Распределение по подразделениям:")
            for subdivision, count in subdivision_stats.head(5).items():
                logger.info(f"   {subdivision}: {count}")
        
        if 'job_employee' in df.columns:
            job_stats = df['job_employee'].value_counts()
            logger.info("📈 Топ-5 должностей:")
            for job, count in job_stats.head(5).items():
                logger.info(f"   {job}: {count}")

    # Остальные методы без изменений...
    def save_employees_to_postgres(self, employees_data: List[Dict], table_name: str = 'mes_employees') -> bool:
        """Сохранение данных сотрудников в PostgreSQL"""
        try:
            if not employees_data:
                logger.warning("📭 Нет данных для сохранения")
                return False
            
            logger.info(f"💾 Сохраняю {len(employees_data)} сотрудников в таблицу '{table_name}'...")
            
            df_employees = pd.DataFrame(employees_data)
            df_employees.to_sql(
                table_name,
                con=self.db_client.engine,
                if_exists='replace',
                index=False,
                method='multi'
            )
            
            logger.success(f"✅ Данные успешно сохранены в таблицу '{table_name}'")
            return True
            
        except Exception as e:
            logger.error(f"💥 Ошибка при сохранении в PostgreSQL: {e}")
            raise

    def save_employees_to_csv(self, employees_data: List[Dict], filename: str = None) -> str:
        """Сохранение данных сотрудников в CSV"""
        try:
            if not employees_data:
                logger.warning("📭 Нет данных для сохранения")
                return ""
            
            if filename is None:
                from datetime import datetime
                timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
                filename = f"exports/employees_{timestamp}.csv"
            
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            
            df_employees = pd.DataFrame(employees_data)
            df_employees.to_csv(filename, encoding='utf-8-sig', index=False)
            
            logger.success(f"💾 Данные сохранены в файл: {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"💥 Ошибка при сохранении в CSV: {e}")
            raise

    def sync_employees(self, save_to_db: bool = True, save_to_csv: bool = True) -> Dict:
        """Синхронизация данных сотрудников"""
        try:
            logger.info("🔄 Начинаю синхронизацию данных сотрудников...")
            
            employees_data = self.get_all_employees_from_api()
            
            results = {
                'total_employees': len(employees_data),
                'db_saved': False,
                'csv_file': None
            }
            
            if save_to_db:
                results['db_saved'] = self.save_employees_to_postgres(employees_data)
            
            if save_to_csv:
                results['csv_file'] = self.save_employees_to_csv(employees_data)
            
            logger.success("✅ Синхронизация сотрудников завершена успешно!")
            return results
            
        except Exception as e:
            logger.error(f"💥 Ошибка при синхронизации сотрудников: {e}")
            raise

    def get_employees_from_postgres(self, table_name: str = 'mes_employees') -> pd.DataFrame:
        """Получение данных сотрудников из PostgreSQL"""
        try:
            logger.info(f"🔍 Получаю данные сотрудников из таблицы '{table_name}'...")
            
            df_employees = pd.read_sql(f'SELECT * FROM {table_name}', con=self.db_client.engine)
            
            logger.info(f"📊 Получено {len(df_employees)} записей сотрудников")
            return df_employees
            
        except Exception as e:
            if "does not exist" in str(e) or "relation" in str(e):
                logger.warning(f"⚠️ Таблица '{table_name}' не существует. Сначала выполните синхронизацию.")
                return pd.DataFrame()
            else:
                logger.error(f"💥 Ошибка при получении данных из PostgreSQL: {e}")
                raise

    def get_employee_statistics(self) -> Dict:
        """Получение статистики по сотрудникам"""
        try:
            df_employees = self.get_employees_from_postgres()
            
            if df_employees.empty:
                return {'error': 'Нет данных о сотрудниках. Выполните синхронизацию: python main_employees.py sync'}
            
            if 'date_dismissal_employee' not in df_employees.columns:
                logger.warning("⚠️ Колонка 'date_dismissal_employee' не найдена. Используем базовую статистику.")
                stats = {
                    'total_employees': len(df_employees),
                    'active_employees': len(df_employees),
                    'dismissed_employees': 0,
                    'active_percentage': 100.0
                }
            else:
                active_employees = df_employees[df_employees['date_dismissal_employee'].isna()]
                dismissed_employees = df_employees[df_employees['date_dismissal_employee'].notna()]
                
                stats = {
                    'total_employees': len(df_employees),
                    'active_employees': len(active_employees),
                    'dismissed_employees': len(dismissed_employees),
                    'active_percentage': round(len(active_employees) / len(df_employees) * 100, 2)
                }
            
            logger.info(f"📊 Статистика: всего {stats['total_employees']}, активных {stats['active_employees']} ({stats['active_percentage']}%)")
            return stats
            
        except Exception as e:
            logger.error(f"💥 Ошибка при получении статистики: {e}")
            raise