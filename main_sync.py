#!/usr/bin/env python3
"""
Основной скрипт для синхронизации рейтингов из различных источников
Использование:
    python main_sync.py [api|csv]
    
Примеры:
    python main_sync.py api    # Синхронизация из MES API
    python main_sync.py csv    # Синхронизация из Google Sheets CSV
    python main_sync.py        # По умолчанию из API
"""

import sys
import argparse
import time
from loguru import logger

from services.rating_syncroniser import RatingsSynchronizer
from utils.helpers import create_directories, format_duration


def setup_logging():
    """Настройка логирования"""
    logger.remove()  # Удаляем стандартный обработчик
    
    # Консольный вывод с цветами
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="INFO"
    )
    
    # Файловый лог
    logger.add(
        "logs/sync_{time:YYYY-MM-DD}.log",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level="DEBUG",
        rotation="1 day",
        retention="30 days"
    )


def create_parser():
    """Создание парсера аргументов командной строки"""
    parser = argparse.ArgumentParser(
        description="Синхронизация рейтингов из различных источников",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
    python main_sync.py api                    # Синхронизация из MES API
    python main_sync.py csv                    # Синхронизация из Google Sheets CSV
    python main_sync.py --source=api          # Альтернативный синтаксис
    python main_sync.py --verbose              # С подробным логированием
        """
    )
    
    parser.add_argument(
        'source',
        nargs='?',
        choices=['api', 'csv'],
        default='api',
        help='Источник данных для синхронизации (по умолчанию: api)'
    )
    
    parser.add_argument(
        '--source',
        choices=['api', 'csv'],
        dest='source_alt',
        help='Альтернативный способ указания источника данных'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Включить подробное логирование (DEBUG уровень)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Режим тестирования без сохранения данных'
    )
    
    return parser


def main():
    """Основная функция"""
    start_time = time.time()
    
    # Создаем необходимые директории
    create_directories()
    
    setup_logging()
    
    parser = create_parser()
    args = parser.parse_args()
    
    # Определяем источник данных
    source = args.source_alt if args.source_alt else args.source
    
    # Настраиваем уровень логирования
    if args.verbose:
        logger.remove()
        logger.add(
            sys.stdout,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
            level="DEBUG"
        )
        logger.add(
            "logs/sync_{time:YYYY-MM-DD}.log",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
            level="DEBUG",
            rotation="1 day",
            retention="30 days"
        )
    
    logger.info("="*60)
    logger.info("🔄 Запуск синхронизации рейтингов")
    logger.info("="*60)
    logger.info(f"📊 Источник данных: {source.upper()}")
    
    if args.dry_run:
        logger.info("🧪 Режим тестирования (данные не будут сохранены)")
    
    try:
        synchronizer = RatingsSynchronizer()
        
        if args.dry_run:
            logger.info("🧪 Режим dry-run: проверка конфигурации...")
            if synchronizer.validate_configuration(source):
                logger.success("✅ Конфигурация корректна!")
            else:
                logger.error("❌ Проблемы с конфигурацией")
                sys.exit(1)
            return
        
        synchronizer.synchronize(source=source)
        
        logger.success("✅ Синхронизация завершена успешно!")
        
    except KeyboardInterrupt:
        logger.warning("⚠️ Операция прервана пользователем")
        sys.exit(130)
    except Exception as e:
        logger.error(f"❌ Критическая ошибка при синхронизации: {e}")
        logger.exception("Подробности ошибки:")
        sys.exit(1)
    finally:
        end_time = time.time()
        duration = format_duration(start_time, end_time)
        logger.info(f"⏱️ Время выполнения: {duration}")
        logger.info("="*60)


if __name__ == '__main__':
    main()