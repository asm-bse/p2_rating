import time
import requests
from typing import List, Dict, Tuple, Optional
from loguru import logger
import pandas as pd
import os

from base.mes_client import MESClient
from database.postgres_client import PostgreSQLClient

class RatingUploader(MESClient):
    """Класс для загрузки рейтингов в MES API"""
    
    def __init__(self):
        super().__init__()
        self.api_url = "https://apimes.starline.ru/v1/ratingrest/create"
        self.batch_size = int(os.getenv('UPLOAD_BATCH_SIZE', 10))
        self.request_delay = float(os.getenv('REQUEST_DELAY', 0.5))
        self.db_client = PostgreSQLClient()

    def post_rating_to_mes(self, data: Dict, retry_count: int = 0) -> Tuple[bool, Optional[str]]:
        """Отправить одну запись рейтинга в MES с повторными попытками"""
        try:
            # Вывод данных для отладки (скрываем слишком длинные поля)
            debug_data = data.copy()
            if 'commentary' in debug_data and debug_data['commentary'] and len(debug_data['commentary']) > 50:
                debug_data['commentary'] = debug_data['commentary'][:50] + '...'
            logger.debug(f"Отправляемые данные: {debug_data}")
            
            headers = {
                'Authorization': f'Bearer {self.token}',
                'Content-Type': 'application/json'
            }
            
            response = requests.post(self.api_url, json=data, headers=headers, timeout=15)
            logger.debug(f"Статус-код ответа: {response.status_code}")
            
            if response.text:
                try:
                    response_json = response.json()
                    logger.debug(f"Ответ сервера: {response_json}")
                except ValueError:
                    logger.debug(f"Ответ сервера (текст): {response.text}")
            
            response.raise_for_status()
            return True, None
            
        except requests.RequestException as e:
            if retry_count < self.max_retries:
                wait_time = (retry_count + 1) * self.retry_delay
                logger.warning(f"Ошибка при отправке ({retry_count + 1}/{self.max_retries}): {e}")
                logger.info(f"Повторная попытка через {wait_time} сек...")
                time.sleep(wait_time)
                return self.post_rating_to_mes(data, retry_count + 1)
            else:
                logger.error(f"Превышено максимальное количество попыток для данных: {debug_data}")
                return False, str(e)

    def process_rating_batch(self, batch_data: List[Dict]) -> Tuple[int, int]:
        """Обработка батча рейтингов"""
        success_count = 0
        error_count = 0
        
        for data in batch_data:
            success, error = self.post_rating_to_mes(data)
            
            if success:
                success_count += 1
                logger.info(f"✅ Рейтинг для сотрудника {data['id_employee']} успешно добавлен")
            else:
                error_count += 1
                logger.error(f"❌ Ошибка для сотрудника {data['id_employee']}: {error}")
            
            # Задержка между запросами
            time.sleep(self.request_delay)
        
        return success_count, error_count

    def get_ratings_from_postgres(self, table_name: str = 'csv_ratings') -> pd.DataFrame:
        """Получение рейтингов из PostgreSQL"""
        try:
            logger.info(f"Получение рейтингов из PostgreSQL таблицы '{table_name}'...")
            df_rating = pd.read_sql(f'SELECT * FROM {table_name}', con=self.db_client.engine)
            
            if df_rating.empty:
                logger.warning(f"Таблица '{table_name}' пуста")
                return df_rating
            
            df_rating = df_rating.sort_values(by='start_date', ascending=False)
            
            # Получение данных за последний период
            latest_period = df_rating['start_date'].max()
            df_latest_ratings = df_rating[df_rating['start_date'] == latest_period]
            
            logger.info(f"Найдено {len(df_latest_ratings)} рейтингов за последний период ({latest_period})")
            return df_latest_ratings
            
        except Exception as e:
            logger.error(f"Ошибка при получении данных из PostgreSQL: {e}")
            raise

    def prepare_api_data(self, row: pd.Series) -> Dict:
        """Подготовка данных для API"""
        return {
            "rating_index": None,  # Будет назначен API
            "id_employee": str(int(row['id_employee'])),
            "start_date": str(row['start_date']),
            "end_date": str(row['end_date']),
            "criterion": str(row['criterion']),
            "mark": int(row['mark']),
            "commentary": str(row['commentary']) if pd.notna(row['commentary']) else "",
            "criterion_index": int(row['criterion_index']) if pd.notna(row['criterion_index']) else 0
        }

    def upload_ratings(self, table_name: str = 'csv_ratings', use_batches: bool = True):
        """Основной метод загрузки рейтингов"""
        try:
            # Получение токена
            self.token = self.get_auth_token()
            
            # Получение данных из PostgreSQL
            df_latest_ratings = self.get_ratings_from_postgres(table_name)
            
            if df_latest_ratings.empty:
                logger.warning("Нет данных для загрузки")
                return
            
            # Подготовка данных для API
            api_data_list = []
            for idx, row in df_latest_ratings.iterrows():
                api_data = self.prepare_api_data(row)
                api_data_list.append(api_data)
            
            total_success = 0
            total_errors = 0
            
            logger.info("\nНачинаю загрузку рейтингов в MES API...")
            
            if use_batches and len(api_data_list) > self.batch_size:
                # Батчевая обработка
                logger.info(f"Обработка {len(api_data_list)} записей батчами по {self.batch_size}")
                
                # Разделение на батчи
                batches = [api_data_list[i:i + self.batch_size] 
                            for i in range(0, len(api_data_list), self.batch_size)]
                
                for batch_num, batch in enumerate(batches, 1):
                    logger.info(f"\n📦 Обработка батча {batch_num}/{len(batches)} ({len(batch)} записей)")
                    
                    batch_success, batch_errors = self.process_rating_batch(batch)
                    total_success += batch_success
                    total_errors += batch_errors
                    
                    logger.info(f"Батч {batch_num}: ✅ {batch_success} успешно, ❌ {batch_errors} ошибок")
                    
                    # Пауза между батчами
                    if batch_num < len(batches):
                        time.sleep(1)
            else:
                # Последовательная обработка
                logger.info("Последовательная обработка записей...")
                for idx, api_data in enumerate(api_data_list, 1):
                    logger.info(f"\nОбработка рейтинга {idx}/{len(api_data_list)}")
                    
                    success, error = self.post_rating_to_mes(api_data)
                    
                    if success:
                        total_success += 1
                        logger.info(f"✅ Рейтинг для сотрудника {api_data['id_employee']} успешно добавлен ({total_success}/{len(api_data_list)})")
                    else:
                        total_errors += 1
                        logger.error(f"❌ Ошибка для сотрудника {api_data['id_employee']}: {error}")
                    
                    time.sleep(self.request_delay)
            
            # Итоги операции
            self.print_summary(total_success, total_errors, len(api_data_list))
            
        except Exception as e:
            logger.error(f"Критическая ошибка при загрузке рейтингов: {e}")
            raise

    def print_summary(self, success_count: int, error_count: int, total_count: int):
        """Вывод итогов операции"""
        success_rate = (success_count / total_count * 100) if total_count > 0 else 0
        
        logger.info("\n" + "="*50)
        logger.info("📊 Итоги операции")
        logger.info("="*50)
        logger.info(f"✅ Успешно загружено: {success_count:>5}")
        logger.info(f"❌ Ошибок загрузки:  {error_count:>5}")
        logger.info(f"📈 Всего обработано: {total_count:>5}")
        logger.info(f"🎯 Успешность:       {success_rate:.1f}%")
        logger.info("="*50)