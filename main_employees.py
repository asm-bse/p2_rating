#!/usr/bin/env python3
"""
Скрипт для работы с данными сотрудников
Использование:
    python main_employees.py sync              # Синхронизация из API
    python main_employees.py stats             # Статистика по сотрудникам
    python main_employees.py export            # Экспорт в CSV
"""

import sys
import argparse
import os
import pandas as pd
from loguru import logger

from services.employee_service import EmployeeService


def setup_logging():
    """Настройка логирования"""
    logger.remove()
    
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="INFO"
    )
    
    logger.add(
        "logs/employees_{time:YYYY-MM-DD}.log",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level="DEBUG",
        rotation="1 day",
        retention="30 days"
    )


def sync_employees(args):
    """Синхронизация данных сотрудников"""
    employee_service = EmployeeService()
    
    results = employee_service.sync_employees(
        save_to_db=not args.no_db,
        save_to_csv=not args.no_csv
    )
    
    logger.info(f"📊 Результаты синхронизации:")
    logger.info(f"   Всего сотрудников: {results['total_employees']}")
    logger.info(f"   Сохранено в БД: {'✅' if results['db_saved'] else '❌'}")
    if results['csv_file']:
        logger.info(f"   CSV файл: {results['csv_file']}")


def show_statistics():
    """Показать статистику по сотрудникам"""
    employee_service = EmployeeService()
    
    try:
        stats = employee_service.get_employee_statistics()
        
        if 'error' in stats:
            logger.error(f"❌ {stats['error']}")
            return
        
        logger.info("📊 Статистика по сотрудникам:")
        logger.info("="*40)
        logger.info(f"👥 Всего сотрудников:     {stats['total_employees']:>6}")
        logger.info(f"✅ Активных сотрудников:  {stats['active_employees']:>6}")
        logger.info(f"❌ Уволенных сотрудников: {stats['dismissed_employees']:>6}")
        logger.info(f"📈 Процент активных:      {stats['active_percentage']:>5.1f}%")
        logger.info("="*40)
        
    except Exception as e:
        if "does not exist" in str(e):
            logger.error("❌ Таблица сотрудников не существует")
            logger.info("💡 Выполните синхронизацию: python main_employees.py sync")
        else:
            logger.error(f"❌ Ошибка при получении статистики: {e}")


def export_employees():
    """Экспорт сотрудников в CSV"""
    employee_service = EmployeeService()
    
    try:
        df_employees = employee_service.get_employees_from_postgres()
        if df_employees.empty:
            logger.warning("❌ Нет данных для экспорта")
            return
        
        filename = employee_service.save_employees_to_csv(df_employees.to_dict('records'))
        logger.success(f"✅ Экспорт завершен: {filename}")
        
    except Exception as e:
        logger.error(f"❌ Ошибка при экспорте: {e}")

def export_current_employees():
    """Экспорт действующих сотрудников с рабочими данными в CSV"""
    employee_service = EmployeeService()
    
    try:
        # Получаем действующих сотрудников (5-значные ID, без даты увольнения)
        df_current_employees = employee_service.get_current_employees_with_work_data()
        
        if df_current_employees.empty:
            logger.warning("❌ Нет данных о действующих сотрудниках для экспорта")
            return
        
        # Сохраняем в CSV
        timestamp = pd.Timestamp.now().strftime('%Y-%m-%d_%H-%M')
        filename = f"exports/current_employees_{timestamp}.csv"
        
        os.makedirs('exports', exist_ok=True)
        df_current_employees.to_csv(filename, index=False, encoding='utf-8-sig')
        
        logger.success(f"✅ Экспорт действующих сотрудников завершен: {filename}")
        logger.info(f"📊 Всего действующих сотрудников: {len(df_current_employees)}")
        
        # Показываем статистику по подразделениям
        if 'subdivision_employee' in df_current_employees.columns:
            subdivision_stats = df_current_employees['subdivision_employee'].value_counts()
            logger.info("📈 Распределение по подразделениям:")
            for subdivision, count in subdivision_stats.head(5).items():
                logger.info(f"   {subdivision}: {count}")
        
        return filename
        
    except Exception as e:
        logger.error(f"❌ Ошибка при экспорте действующих сотрудников: {e}")
        return None
    
def main():
    parser = argparse.ArgumentParser(description="Управление данными сотрудников")
    
    subparsers = parser.add_subparsers(dest='command', help='Доступные команды')
    
    # Команда синхронизации
    sync_parser = subparsers.add_parser('sync', help='Синхронизация из API')
    sync_parser.add_argument('--no-db', action='store_true', help='Не сохранять в БД')
    sync_parser.add_argument('--no-csv', action='store_true', help='Не сохранять в CSV')
    
    # Команда статистики
    subparsers.add_parser('stats', help='Показать статистику')
    
    # Команда экспорта
    subparsers.add_parser('export', help='Экспорт в CSV')
    
    # Команда экспорта действующих сотрудников
    subparsers.add_parser('current', help='Экспорт действующих сотрудников с рабочими данными')
    
    args = parser.parse_args()
    
    setup_logging()
    
    logger.info("="*50)
    logger.info("👥 Управление данными сотрудников")
    logger.info("="*50)
    
    try:
        if args.command == 'sync':
            sync_employees(args)
        elif args.command == 'stats':
            show_statistics()
        elif args.command == 'export':
            export_employees()
        elif args.command == 'current':
            export_current_employees()
        else:
            parser.print_help()
            
    except KeyboardInterrupt:
        logger.warning("⚠️ Операция прервана пользователем")
        sys.exit(130)
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()