import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

@dataclass
class APIConfig:
    """Конфигурация для MES API"""
    base_url: str = "https://apimes.starline.ru/v1"
    username: str = os.getenv('MES_USERNAME')
    password: str = os.getenv('MES_PASSWORD')
    max_retries: int = int(os.getenv('MAX_RETRIES', 3))
    retry_delay: float = float(os.getenv('RETRY_DELAY', 1.0))
    request_delay: float = float(os.getenv('REQUEST_DELAY', 0.5))

@dataclass
class DatabaseConfig:
    """Конфигурация для PostgreSQL"""
    user: str = os.environ['POSTGRES_USER']
    password: str = os.environ['POSTGRES_PASSWORD']
    host: str = os.environ['POSTGRES_HOST']
    port: str = os.environ['POSTGRES_PORT']
    database: str = os.environ['POSTGRES_DB_NAME']

@dataclass
class AppConfig:
    """Основная конфигурация приложения"""
    api: APIConfig = APIConfig()
    database: DatabaseConfig = DatabaseConfig()
    upload_batch_size: int = int(os.getenv('UPLOAD_BATCH_SIZE', 10))