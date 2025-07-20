"""
Вспомогательные функции для работы с рейтингами
"""

import os
import time
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import pandas as pd
from loguru import logger


def create_directories():
    """Создание необходимых директорий"""
    directories = ['logs', 'data', 'exports']
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)
            logger.info(f"Создана директория: {directory}")


def format_file_size(size_bytes: int) -> str:
    """Форматирование размера файла в человекочитаемый вид"""
    if size_bytes == 0:
        return "0B"
    
    size_names = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while size_bytes >= 1024 and i < len(size_names) - 1:
        size_bytes /= 1024.0
        i += 1
    
    return f"{size_bytes:.1f}{size_names[i]}"


def format_duration(start_time: float, end_time: float) -> str:
    """Форматирование длительности операции"""
    duration = end_time - start_time
    
    if duration < 60:
        return f"{duration:.1f} сек"
    elif duration < 3600:
        minutes = int(duration // 60)
        seconds = duration % 60
        return f"{minutes} мин {seconds:.1f} сек"
    else:
        hours = int(duration // 3600)
        minutes = int((duration % 3600) // 60)
        return f"{hours} ч {minutes} мин"


def safe_get_env(key: str, default: Any = None, required: bool = False) -> str:
    """Безопасное получение переменной окружения"""
    value = os.getenv(key, default)
    
    if required and value is None:
        raise ValueError(f"Обязательная переменная окружения отсутствует: {key}")
    
    return value


def validate_dataframe(df: pd.DataFrame, required_columns: List[str]) -> bool:
    """Валидация DataFrame на наличие обязательных колонок"""
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        logger.error(f"Отсутствуют обязательные колонки: {missing_columns}")
        return False
    
    logger.debug(f"DataFrame валидация пройдена. Колонки: {list(df.columns)}")
    return True


def clean_string(value: str) -> str:
    """Очистка строки от лишних символов"""
    if pd.isna(value) or value is None:
        return ""
    
    return str(value).strip()


def safe_int_convert(value: Any, default: int = 0) -> int:
    """Безопасное преобразование в int"""
    try:
        if pd.isna(value) or value is None or value == '':
            return default
        return int(float(value))
    except (ValueError, TypeError):
        logger.warning(f"Не удалось преобразовать '{value}' в int, использую значение по умолчанию: {default}")
        return default


def safe_float_convert(value: Any, default: float = 0.0) -> float:
    """Безопасное преобразование в float"""
    try:
        if pd.isna(value) or value is None or value == '':
            return default
        return float(value)
    except (ValueError, TypeError):
        logger.warning(f"Не удалось преобразовать '{value}' в float, использую значение по умолчанию: {default}")
        return default


def get_date_range_description(start_date: str, end_date: str) -> str:
    """Получение описания периода дат"""
    try:
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        if start == end:
            return f"за {start.strftime('%d.%m.%Y')}"
        else:
            return f"с {start.strftime('%d.%m.%Y')} по {end.strftime('%d.%m.%Y')}"
    except ValueError:
        return f"с {start_date} по {end_date}"


def chunk_list(lst: List[Any], chunk_size: int) -> List[List[Any]]:
    """Разделение списка на части заданного размера"""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]


def retry_operation(operation, max_retries: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """Декоратор для повторных попыток выполнения операции"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        logger.error(f"Операция '{func.__name__}' не удалась после {max_retries} попыток: {e}")
                        raise
                    
                    wait_time = delay * (backoff ** attempt)
                    logger.warning(f"Попытка {attempt + 1}/{max_retries} операции '{func.__name__}' не удалась: {e}")
                    logger.info(f"Повторная попытка через {wait_time:.1f} сек...")
                    time.sleep(wait_time)
            
        return wrapper
    return decorator