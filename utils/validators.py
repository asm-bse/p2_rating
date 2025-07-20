"""
Валидаторы для данных рейтингов
"""

import re
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import pandas as pd
from loguru import logger


class RatingValidator:
    """Валидатор данных рейтингов"""
    
    # Допустимые критерии рейтинга
    VALID_CRITERIA = {
        'performance', 'quality', 'predlozheniya', 'can study', 'can teach',
        'discipline', 'stage', 'oborudovanie', 'own_skills', 'other_skills',
        'ZOG', 'academy', 'points_sum', 'rank', 'new_performance', 'new_quality',
        'discipline_and_oborudovanie', 'new_skills'
    }
    
    # Диапазоны оценок для разных критериев
    MARK_RANGES = {
        'performance': (0, 5),
        'quality': (0, 5),
        'predlozheniya': (0, 5),
        'can study': (0, 1),
        'can teach': (0, 1),
        'discipline': (0, 5),
        'stage': (0, 10),
        'oborudovanie': (0, 5),
        'own_skills': (0, 5),
        'other_skills': (0, 5),
        'ZOG': (0, 100),
        'academy': (0, 100),
        'points_sum': (0, 1000),
        'rank': (1, 100),
        'new_performance': (0, 5),
        'new_quality': (0, 5),
        'discipline_and_oborudovanie': (0, 5),
        'new_skills': (0, 5)
    }

    @classmethod
    def validate_employee_id(cls, employee_id: str) -> bool:
        """Валидация ID сотрудника"""
        if not employee_id or pd.isna(employee_id):
            return False
        
        try:
            emp_id = int(float(str(employee_id)))
            return emp_id > 0
        except (ValueError, TypeError):
            return False

    @classmethod
    def validate_date(cls, date_str: str) -> bool:
        """Валидация даты в формате YYYY-MM-DD"""
        if not date_str or pd.isna(date_str):
            return False
        
        try:
            datetime.strptime(str(date_str), '%Y-%m-%d')
            return True
        except ValueError:
            return False

    @classmethod
    def validate_criterion(cls, criterion: str) -> bool:
        """Валидация критерия рейтинга"""
        if not criterion or pd.isna(criterion):
            return False
        
        return str(criterion).strip() in cls.VALID_CRITERIA

    @classmethod
    def validate_mark(cls, mark: any, criterion: str) -> bool:
        """Валидация оценки для конкретного критерия"""
        if pd.isna(mark):
            return False
        
        try:
            mark_value = float(mark)
            
            if criterion in cls.MARK_RANGES:
                min_val, max_val = cls.MARK_RANGES[criterion]
                return min_val <= mark_value <= max_val
            else:
                # Для неизвестных критериев используем общий диапазон
                return 0 <= mark_value <= 100
                
        except (ValueError, TypeError):
            return False

    @classmethod
    def validate_rating_record(cls, record: Dict) -> Tuple[bool, List[str]]:
        """Валидация одной записи рейтинга"""
        errors = []
        
        # Проверка обязательных полей
        required_fields = ['id_employee', 'start_date', 'end_date', 'criterion', 'mark']
        for field in required_fields:
            if field not in record:
                errors.append(f"Отсутствует обязательное поле: {field}")
        
        if errors:
            return False, errors
        
        # Валидация ID сотрудника
        if not cls.validate_employee_id(record['id_employee']):
            errors.append(f"Некорректный ID сотрудника: {record['id_employee']}")
        
        # Валидация дат
        if not cls.validate_date(record['start_date']):
            errors.append(f"Некорректная дата начала: {record['start_date']}")
        
        if not cls.validate_date(record['end_date']):
            errors.append(f"Некорректная дата окончания: {record['end_date']}")
        
        # Валидация критерия
        if not cls.validate_criterion(record['criterion']):
            errors.append(f"Некорректный критерий: {record['criterion']}")
        
        # Валидация оценки
        if not cls.validate_mark(record['mark'], record['criterion']):
            errors.append(f"Некорректная оценка {record['mark']} для критерия {record['criterion']}")
        
        # Проверка логики дат
        try:
            start_date = datetime.strptime(str(record['start_date']), '%Y-%m-%d')
            end_date = datetime.strptime(str(record['end_date']), '%Y-%m-%d')
            
            if start_date > end_date:
                errors.append("Дата начала не может быть больше даты окончания")
                
        except ValueError:
            pass  # Ошибки дат уже зафиксированы выше
        
        return len(errors) == 0, errors

    @classmethod
    def validate_dataframe(cls, df: pd.DataFrame) -> Tuple[bool, Dict]:
        """Валидация DataFrame с рейтингами"""
        validation_results = {
            'total_records': len(df),
            'valid_records': 0,
            'invalid_records': 0,
            'errors': [],
            'warnings': []
        }
        
        if df.empty:
            validation_results['errors'].append("DataFrame пуст")
            return False, validation_results
        
        # Проверка обязательных колонок
        required_columns = ['id_employee', 'start_date', 'end_date', 'criterion', 'mark']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            validation_results['errors'].append(f"Отсутствуют обязательные колонки: {missing_columns}")
            return False, validation_results
        
        # Валидация каждой записи
        for idx, row in df.iterrows():
            record = row.to_dict()
            is_valid, errors = cls.validate_rating_record(record)
            
            if is_valid:
                validation_results['valid_records'] += 1
            else:
                validation_results['invalid_records'] += 1
                validation_results['errors'].extend([f"Строка {idx}: {error}" for error in errors])
        
        # Статистика по критериям
        criterion_stats = df['criterion'].value_counts()
        unknown_criteria = set(df['criterion'].unique()) - cls.VALID_CRITERIA
        
        if unknown_criteria:
            validation_results['warnings'].append(f"Обнаружены неизвестные критерии: {unknown_criteria}")
        
        validation_results['criterion_stats'] = criterion_stats.to_dict()
        
        success_rate = validation_results['valid_records'] / validation_results['total_records']
        is_valid = success_rate >= 0.95  # 95% записей должны быть валидными
        
        return is_valid, validation_results


def validate_api_response(response_data: Dict) -> bool:
    """Валидация ответа API"""
    if not response_data:
        return False
    
    # Проверяем наличие ключевых полей в ответе
    if 'rating_index' in response_data:
        return True
    
    # Проверяем на ошибки
    if 'error' in response_data or 'message' in response_data:
        logger.warning(f"API вернул ошибку: {response_data}")
        return False
    
    return True


def sanitize_filename(filename: str) -> str:
    """Очистка имени файла от недопустимых символов"""
    # Удаляем недопустимые символы для имени файла
    sanitized = re.sub(r'[<>:"/\\|?*]', '_', filename)
    
    # Ограничиваем длину
    if len(sanitized) > 200:
        sanitized = sanitized[:200]
    
    return sanitized


def validate_connection_params(params: Dict) -> bool:
    """Валидация параметров подключения к базе данных"""
    required_params = ['host', 'port', 'user', 'password', 'database']
    
    for param in required_params:
        if param not in params or not params[param]:
            logger.error(f"Отсутствует обязательный параметр подключения: {param}")
            return False
    
    return True