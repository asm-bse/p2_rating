import os
import pandas as pd
from sqlalchemy import create_engine
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

class PostgreSQLClient:
    """Клиент для работы с PostgreSQL"""
    
    def __init__(self):
        try:
            pg_user = os.environ['POSTGRES_USER']
            pg_pass = os.environ['POSTGRES_PASSWORD']
            pg_host = os.environ['POSTGRES_HOST']
            pg_port = os.environ['POSTGRES_PORT']
            pg_db = os.environ['POSTGRES_DB_NAME']
            
            self.engine = create_engine(
                f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
            )
            logger.info("PostgreSQL подключение настроено успешно")
        except KeyError as e:
            logger.error(f"Отсутствует обязательная переменная окружения для PostgreSQL: {e}")
            raise
        except Exception as e:
            logger.error(f"Ошибка при создании подключения к PostgreSQL: {e}")
            raise

    def read_table(self, table_name: str) -> pd.DataFrame:
        """Чтение таблицы из PostgreSQL"""
        try:
            logger.info(f"📖 Получение данных из таблицы '{table_name}'...")
            df = pd.read_sql(f'SELECT * FROM {table_name}', con=self.engine)
            logger.info(f"✅ Загружено {len(df)} записей из таблицы '{table_name}'")
            return df
        except Exception as e:
            logger.error(f"❌ Ошибка при получении данных из PostgreSQL: {e}")
            raise

    def save_table(self, df: pd.DataFrame, table_name: str, if_exists: str = 'replace'):
        """Сохранение DataFrame в PostgreSQL"""
        try:
            logger.info(f"💾 Сохранение {len(df)} записей в таблицу '{table_name}'...")
            df.to_sql(table_name, con=self.engine, if_exists=if_exists, index=False)
            logger.info("✅ Сохранение завершено успешно")
        except Exception as e:
            logger.error(f"❌ Ошибка при сохранении в PostgreSQL: {e}")
            raise