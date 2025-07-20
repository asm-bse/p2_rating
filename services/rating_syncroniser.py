import os
import requests
import pandas as pd
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from loguru import logger
import json
import time

from base.mes_client import MESClient
from database.postgres_client import PostgreSQLClient

class RatingsSynchronizer(MESClient):
    """Класс для синхронизации рейтингов из различных источников"""
    
    def __init__(self):
        super().__init__()
        self.api_url = os.getenv('MES_API_URL', 'https://apimes.starline.ru/v1/ratingrest')
        self.page_size = int(os.getenv('MES_PAGE_SIZE', 20))
        self.parallel_requests = int(os.getenv('PARALLEL_REQUESTS', 20))
        self.csv_url = os.getenv('GOOGLE_SHEETS_CSV_URL')
        self.db_client = PostgreSQLClient()


    def validate_configuration(self, source: str) -> bool:
        """Валидация конфигурации для указанного источника"""
        if source == 'csv':
            if not self.csv_url:
                logger.error("❌ Для работы с CSV необходимо установить GOOGLE_SHEETS_CSV_URL в .env файле")
                return False
            logger.info(f"✅ CSV URL настроен: {self.csv_url}")
        
        elif source == 'api':
            if not self.username or not self.password:
                logger.error("❌ Для работы с API необходимо установить MES_USERNAME и MES_PASSWORD в .env файле")
                return False
            logger.info(f"✅ API учетные данные настроены для пользователя: {self.username}")
        
        return True
    

    def fetch_page_with_retry(self, page: int) -> Optional[List[Dict]]:
        """Загрузка одной страницы с повторными попытками"""
        for attempt in range(self.max_retries):
            try:
                logger.debug(f"📄 Загружаю страницу {page} (попытка {attempt + 1})")
                params = {'page': page, 'size': self.page_size}
                headers = {'Authorization': f'Bearer {self.token}'}
                resp = requests.get(self.api_url, params=params, headers=headers, timeout=15)
                resp.raise_for_status()
                data = resp.json()
                
                # Ожидаем, что в ответе есть ключ 'items' или сам ответ — список
                items = data.get('items') if isinstance(data, dict) and 'items' in data else data
                
                if items:
                    logger.debug(f"✅ Страница {page}: загружено {len(items)} записей")
                else:
                    logger.debug(f"🔚 Страница {page}: пустая")
                
                return items
                
            except requests.RequestException as e:
                logger.warning(f"❌ Ошибка при запросе страницы {page}, попытка {attempt + 1}: {e}")
                if attempt < self.max_retries - 1:
                    wait_time = self.retry_delay * (attempt + 1)
                    logger.debug(f"⏳ Ожидание {wait_time}s перед повторной попыткой...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"💥 Не удалось загрузить страницу {page} после {self.max_retries} попыток")
                    return None

    def pages_are_identical(self, page1: List[Dict], page2: List[Dict]) -> bool:
        """Проверяем идентичность двух страниц"""
        if not page1 or not page2:
            return False
        if len(page1) != len(page2):
            return False
        return json.dumps(page1, sort_keys=True) == json.dumps(page2, sort_keys=True)

    def fetch_all_ratings(self) -> List[Dict]:
        """
        Загрузка всех страниц рейтингов из MES API с параллельными запросами
        """
        all_records = []
        page = 1
        previous_page_data = None
        
        # Получаем токен
        self.token = self.get_auth_token()
        
        logger.info("🔄 Начинаем загрузку данных из MES API...")
        
        while True:
            # Определяем диапазон страниц для параллельной загрузки
            page_range = list(range(page, page + self.parallel_requests))
            
            logger.info(f"📦 Загружаю батч страниц: {page_range[0]}-{page_range[-1]} ({len(page_range)} страниц)")
            
            # Параллельная загрузка страниц
            with ThreadPoolExecutor(max_workers=self.parallel_requests) as executor:
                future_to_page = {
                    executor.submit(self.fetch_page_with_retry, p): p 
                    for p in page_range
                }
                
                batch_results = {}
                completed_count = 0
                
                for future in as_completed(future_to_page):
                    page_num = future_to_page[future]
                    completed_count += 1
                    
                    try:
                        result = future.result()
                        batch_results[page_num] = result
                        logger.debug(f"📊 Обработана страница {page_num} ({completed_count}/{len(page_range)})")
                    except Exception as e:
                        logger.error(f"💥 Ошибка при обработке страницы {page_num}: {e}")
                        batch_results[page_num] = None
            
            # Обработка результатов в порядке страниц
            batch_records = 0
            for p in page_range:
                items = batch_results.get(p)
                
                if items is None:
                    logger.error(f"❌ Не удалось загрузить страницу {p}")
                    logger.info(f"📈 Загрузка завершена. Всего записей: {len(all_records)}")
                    return all_records
                
                if not items:
                    logger.info(f"🔚 Страницы закончились на странице {p}. Всего записей: {len(all_records)}")
                    return all_records
                
                # Проверяем на совпадение с предыдущей страницей
                if previous_page_data and self.pages_are_identical(items, previous_page_data):
                    logger.info(f"🔄 Страница {p} идентична предыдущей. Загрузка завершена. Всего записей: {len(all_records)}")
                    return all_records
                
                batch_records += len(items)
                all_records.extend(items)
                previous_page_data = items
            
            logger.info(f"✅ Батч завершен: +{batch_records} записей. Всего: {len(all_records)}")
            
            page += self.parallel_requests
            time.sleep(0.1)  # Небольшая пауза между батчами

    def save_to_postgres(self, df: pd.DataFrame, table_name: str = 'mes_ratings'):
        """
        Сохранить DataFrame в PostgreSQL.
        По умолчанию перезаписывает таблицу.
        """
        logger.info(f"💾 Запись {len(df)} строк в таблицу '{table_name}' PostgreSQL...")
        try:
            # Используем db_client.engine вместо self.engine_pg
            self.db_client.save_table(df, table_name, if_exists='replace')
            logger.info("✅ Запись в PostgreSQL завершена успешно")
        except Exception as e:
            logger.error(f"❌ Ошибка при записи в PostgreSQL: {e}")
            raise

    def criterion_index_mapping(self, criterion: str) -> int:
        """Маппинг критериев на индексы"""
        mapping = {
            'performance': 1,
            'quality': 2,
            'predlozheniya': 3,
            'can study': 4,
            'can teach': 5,
            'discipline': 6,
            'stage': 7,
            'oborudovanie': 8,
            'own_skills': 9,
            'other_skills': 10,
            'ZOG': 11,
            'academy': 12,
            'points_sum': 13,
            'rank': 14,
            'new_performance': 20,
            'new_quality': 21,
            'discipline_and_oborudovanie': 22,
            'new_skills': 23
        }
        return mapping.get(criterion, 777)

    def load_ratings_from_csv(self) -> pd.DataFrame:
        """Загрузка рейтингов из Google Sheets CSV"""
        logger.info('📊 Запущена загрузка рейтинга из CSV')
        
        if not self.csv_url:
            raise ValueError("GOOGLE_SHEETS_CSV_URL не установлен в переменных окружения. Проверьте .env файл.")
        
        try:
            # Загрузка данных из Google Sheets
            logger.info(f"🌐 Загружаю данные из: {self.csv_url}")
            df_rating = pd.read_csv(self.csv_url, skiprows=1)
            logger.info(f'📥 Данные из Google Sheets выгружены: {len(df_rating)} строк')
            
            # Очистка названий колонок
            df_rating.columns = df_rating.columns.to_series().apply(lambda x: x.strip())
            logger.debug(f"📋 Колонки: {list(df_rating.columns)}")
            
            # Обработка данных
            df_rating = df_rating.fillna(0)
            df_rating = df_rating.drop('fio', axis=1)
            df_rating = pd.melt(
                df_rating, 
                id_vars=['id_employee', 'start_date', 'end_date'], 
                var_name='criterion', 
                value_name='mark'
            )
            logger.info(f"🔄 После melt: {len(df_rating)} строк")
            
            df_rating['mark'] = df_rating['mark'].astype(int)
            df_rating['commentary'] = ''
            df_rating.index.names = ['rating_index']
            
            # Применение маппинга критериев
            df_rating['criterion_index'] = df_rating['criterion'].apply(self.criterion_index_mapping)
            
            # Фильтрация неизвестных критериев
            before_filter = len(df_rating)
            df_rating = df_rating[df_rating['criterion_index'] != 777]
            after_filter = len(df_rating)
            
            if before_filter != after_filter:
                logger.warning(f"🗑️  Отфильтровано {before_filter - after_filter} записей с неизвестными критериями")
            
            logger.info(f'✅ Обработано {len(df_rating)} записей рейтинга')
            return df_rating
            
        except Exception as e:
            logger.error(f'❌ Ошибка при загрузке CSV: {e}')
            raise

    def append_ratings_to_postgres(self, df: pd.DataFrame):
        """Сохранение рейтингов в PostgreSQL с инкрементальными индексами"""
        
        logger.info("🔍 Получение максимального индекса из существующих данных...")
        
        # Получение максимального индекса из существующих данных
        try:
            df_rating_old = self.db_client.read_table('mes_ratings')
            max_index = df_rating_old.index.max() if not df_rating_old.empty else 0
            logger.debug(f'📊 Максимальный существующий индекс: {max_index}')
        except Exception as e:
            logger.warning(f'⚠️  Не удалось получить существующие данные из PostgreSQL: {e}')
            max_index = 0
        
        try:
            # Установка новых индексов
            df = df.reset_index(drop=True)
            df.index = df.index + max_index + 1
            
            logger.info(f"💾 Сохранение {len(df)} записей с индексами {max_index + 1}-{max_index + len(df)}...")
            
            # Сохранение в PostgreSQL
            df.to_sql(
                con=self.db_client.engine, 
                name='mes_ratings', 
                if_exists='append', 
                index=True, 
                index_label="rating_index"
            )
            logger.info(f'✅ Рейтинг загружен в PostgreSQL: {len(df)} записей')
        except Exception as e:
            logger.error(f'❌ Ошибка при сохранении в PostgreSQL: {e}')
            raise

    def synchronize(self, source: str = 'api'):
        """
        Основной метод синхронизации
        source: 'api' - загрузка из MES API, 'csv' - загрузка из Google Sheets CSV
        """
        logger.info(f"🚀 Начата синхронизация из источника: {source.upper()}")
        
        # Валидация конфигурации
        if not self.validate_configuration(source):
            raise ValueError(f"Неверная конфигурация для источника: {source}")
        
        try:
            if source == 'api':
                # Получить все записи из API
                logger.info("📡 Получение данных из MES API...")
                records = self.fetch_all_ratings()

                if not records:
                    logger.warning("⚠️  Нет данных для сохранения из API")
                    return

                # Преобразовать в DataFrame
                logger.info("🔄 Преобразование в DataFrame...")
                df = pd.DataFrame(records)
                logger.info(f"✅ Преобразование в DataFrame выполнено: {len(df)} записей")

                # Сохранить в Postgres
                self.save_to_postgres(df)
                
            elif source == 'csv':
                # Загрузка из CSV
                logger.info("📊 Загрузка из CSV...")
                df = self.load_ratings_from_csv()
                
                if df.empty:
                    logger.warning("⚠️  Нет данных для сохранения из CSV")
                    return
                
                # Сохранить в PostgreSQL
                logger.info("💾 Сохранение в csv_ratings...")
                self.save_to_postgres(df, table_name='csv_ratings')
                
                # Добавить с инкрементальными индексами
                logger.info("📈 Добавление с инкрементальными индексами...")
                self.append_ratings_to_postgres(df)
                
            else:
                raise ValueError("source должен быть 'api' или 'csv'")
                
            logger.success("🎉 Синхронизация завершена успешно!")
            
        except Exception as e:
            logger.error(f"❌ Ошибка при синхронизации: {e}")
            raise