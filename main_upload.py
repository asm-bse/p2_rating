#!/usr/bin/env python3
"""
Основной скрипт для загрузки рейтингов в MES API
Использование:
    python main_upload.py [table_name] [batch_mode]
    
Примеры:
    python main_upload.py                           # Загрузка из csv_ratings с батчами
    python main_upload.py mes_ratings               # Загрузка из mes_ratings с батчами  
    python main_upload.py csv_ratings false         # Загрузка из csv_ratings без батчей
    python main_upload.py --table=mes_ratings       # Альтернативный синтаксис
    python main_upload.py --no-batch                # Отключить батчевую обработку
"""

import sys
import argparse
from loguru import logger

from services.rating_uploader import RatingUploader


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
        "logs/upload_{time:YYYY-MM-DD}.log",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level="DEBUG",
        rotation="1 day",
        retention="30 days"
    )


def create_parser():
    """Создание парсера аргументов командной строки"""
    parser = argparse.ArgumentParser(
        description="Загрузка рейтингов в MES API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
    python main_upload.py                           # Загрузка из csv_ratings с батчами
    python main_upload.py mes_ratings               # Загрузка из mes_ratings с батчами  
    python main_upload.py csv_ratings --no-batch    # Загрузка без батчей
    python main_upload.py --table=mes_ratings       # Альтернативный синтаксис
    python main_upload.py --verbose                 # С подробным логированием
        """
    )
    
    parser.add_argument(
        'table_name',
        nargs='?',
        default='csv_ratings',
        help='Имя таблицы в PostgreSQL для загрузки данных (по умолчанию: csv_ratings)'
    )
    
    parser.add_argument(
        'batch_mode',
        nargs='?',
        choices=['true', '1', 'yes', 'y', 'false', '0', 'no', 'n'],
        default='true',
        help='Использовать батчевую обработку (по умолчанию: true)'
    )
    
    parser.add_argument(
        '--table', '-t',
        dest='table_alt',
        help='Альтернативный способ указания имени таблицы'
    )
    
    parser.add_argument(
        '--no-batch',
        action='store_true',
        help='Отключить батчевую обработку'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        default=10,
        help='Размер батча для обработки (по умолчанию: 10)'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Включить подробное логирование (DEBUG уровень)'
    )
    
    parser.add_argument(
        '--force', '-f',
        action='store_true',
        help='Пропустить подтверждение перед загрузкой'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Режим тестирования без отправки данных в API'
    )
    
    return parser


def confirm_upload(record_count: int, table_name: str, use_batches: bool, force: bool = False) -> bool:
    """Подтверждение операции загрузки"""
    if force:
        logger.info("Принудительный режим: пропускаем подтверждение")
        return True
    
    batch_mode = "батчами" if use_batches else "последовательно"
    
    logger.info("\n" + "="*50)
    logger.info("📋 Параметры загрузки:")
    logger.info("="*50)
    logger.info(f"📊 Таблица:          {table_name}")
    logger.info(f"📈 Количество записей: {record_count}")
    logger.info(f"⚙️  Режим обработки:  {batch_mode}")
    logger.info("="*50)
    
    while True:
        try:
            confirm = input("\n🤔 Продолжить загрузку? (y/n): ").lower().strip()
            if confirm in ['y', 'yes', 'да']:
                return True
            elif confirm in ['n', 'no', 'нет']:
                return False
            else:
                print("Пожалуйста, введите 'y' (да) или 'n' (нет)")
        except KeyboardInterrupt:
            print("\n")
            logger.warning("Операция прервана пользователем")
            return False


def main():
    """Основная функция"""
    setup_logging()
    
    parser = create_parser()
    args = parser.parse_args()
    
    # Определяем имя таблицы
    table_name = args.table_alt if args.table_alt else args.table_name
    
    # Определяем режим батчей
    if args.no_batch:
        use_batches = False
    else:
        use_batches = args.batch_mode.lower() in ['true', '1', 'yes', 'y']
    
    # Настраиваем уровень логирования
    if args.verbose:
        logger.remove()
        logger.add(
            sys.stdout,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
            level="DEBUG"
        )
        logger.add(
            "logs/upload_{time:YYYY-MM-DD}.log",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
            level="DEBUG",
            rotation="1 day",
            retention="30 days"
        )
    
    logger.info("="*60)
    logger.info("🚀 Запуск загрузки рейтингов в MES API")
    logger.info("="*60)
    
    if args.dry_run:
        logger.info("🧪 Режим тестирования (данные не будут отправлены в API)")
    
    try:
        uploader = RatingUploader()
        
        # Устанавливаем размер батча, если указан
        if args.batch_size != 10:
            uploader.batch_size = args.batch_size
            logger.info(f"📦 Размер батча установлен: {args.batch_size}")
        
        # Получаем данные для предварительного просмотра
        df_ratings = uploader.get_ratings_from_postgres(table_name)
        
        if df_ratings.empty:
            logger.warning("❌ Нет данных для загрузки")
            sys.exit(0)
        
        # Подтверждение операции
        if not confirm_upload(len(df_ratings), table_name, use_batches, args.force):
            logger.info("🚫 Операция отменена пользователем")
            sys.exit(0)
        
        # Выполняем загрузку
        if args.dry_run:
            logger.info("🧪 Режим dry-run: имитация загрузки...")
            logger.info(f"📊 Было бы загружено {len(df_ratings)} записей")
            logger.success("✅ Тестирование завершено успешно!")
        else:
            uploader.upload_ratings(table_name=table_name, use_batches=use_batches)
            logger.success("✅ Загрузка завершена успешно!")
        
    except KeyboardInterrupt:
        logger.warning("⚠️ Операция прервана пользователем")
        sys.exit(130)
    except Exception as e:
        logger.error(f"❌ Критическая ошибка при загрузке: {e}")
        logger.exception("Подробности ошибки:")
        sys.exit(1)
    finally:
        logger.info("="*60)


if __name__ == "__main__":
    main()