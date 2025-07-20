import os
import requests
import time
from abc import ABC, abstractmethod
from typing import Optional
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

class MESClient(ABC):
    """Базовый клиент для работы с MES API"""
    
    def __init__(self):
        self.auth_url = "https://apimes.starline.ru/v1/auth/login"
        self.username = os.getenv('MES_USERNAME')
        self.password = os.getenv('MES_PASSWORD')
        self.max_retries = int(os.getenv('MAX_RETRIES', 3))
        self.retry_delay = float(os.getenv('RETRY_DELAY', 1.0))
        self.token = None

    def get_auth_token(self) -> str:
        """Получаем OAuth токен"""
        try:
            logger.info("Получение токена авторизации...")
            response = requests.post(
                self.auth_url,
                json={
                    "username": self.username,
                    "password": self.password,
                    "grant_type": "password",
                    "client_id": "testclient",
                    "client_secret": "testpass",
                },
                headers={"Content-Type": "application/json"},
                timeout=10
            )
            response.raise_for_status()
            
            if response.status_code != 200:
                raise Exception(response.json())
            if response.json().get('error_description'):
                raise Exception(response.json().get('error_description'))
            if response.json().get('password'):
                raise Exception(response.json().get('password')[0])
                
            logger.info("Токен получен успешно")
            self.token = response.json().get('access_token')
            return self.token
        except Exception as e:
            logger.error(f"Ошибка получения токена: {e}")
            raise

    def get_headers(self) -> dict:
        """Получить заголовки для авторизованных запросов"""
        if not self.token:
            self.get_auth_token()
        return {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json'
        }