#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Модуль для работы с API сервиса «Поиск Налогоплательщика».
"""

from __future__ import annotations

from typing import Dict, Any, Optional, Literal
import requests
from enum import Enum


class TaxpayerType(str, Enum):
    """Типы налогоплательщиков"""
    IP = "IP"  # Индивидуальный предприниматель
    LZCHP = "LZCHP"  # Лицо, занимающееся частной практикой
    UL = "UL"  # Юридическое лицо


class TaxpayerAPIClient:
    """Клиент для работы с API поиска налогоплательщика"""
    
    def __init__(
        self,
        portal_host: str,
        portal_token: str
    ):
        """
        Инициализация клиента API.
        
        Args:
            portal_host: Базовый URL портала (например, https://portal.example.com)
            portal_token: Токен X-Portal-Token
        """
        self.portal_host = portal_host.rstrip('/')
        self.portal_token = portal_token
        self.base_url = f"{self.portal_host}/services/isnaportalsync/public/taxpayer-data"
    
    def search_taxpayer(
        self,
        taxpayer_code: str,
        taxpayer_type: TaxpayerType,
        name: Optional[str] = None,
        first_name: Optional[str] = None,
        last_name: Optional[str] = None,
        print: bool = False
    ) -> Dict[str, Any]:
        """
        Поиск налогоплательщика по данным.
        
        Args:
            taxpayer_code: ИИН/БИН налогоплательщика (12 цифр)
            taxpayer_type: Тип налогоплательщика (IP, LZCHP, UL)
            name: Наименование (для ИП и ЮЛ)
            first_name: Имя (для ЛЗЧП)
            last_name: Фамилия (для ЛЗЧП)
            print: Если True, возвращает PDF в base64, иначе JSON
        
        Returns:
            Словарь с результатами поиска
        
        Raises:
            requests.RequestException: При ошибке HTTP запроса
            ValueError: При неверных параметрах
        """
        # Валидация параметров
        if not taxpayer_code or len(taxpayer_code) != 12 or not taxpayer_code.isdigit():
            raise ValueError("taxpayer_code должен быть строкой из 12 цифр")
        
        if taxpayer_type == TaxpayerType.LZCHP:
            if not first_name or not last_name:
                raise ValueError("Для ЛЗЧП необходимо указать first_name и last_name")
        elif taxpayer_type in (TaxpayerType.IP, TaxpayerType.UL):
            if not name:
                raise ValueError(f"Для {taxpayer_type.value} необходимо указать name")
        
        # Подготовка параметров запроса
        params = {
            "taxpayerCode": taxpayer_code,
            "taxpayerType": taxpayer_type.value,
            "print": "true" if print else "false"
        }
        
        # Добавление специфичных параметров в зависимости от типа
        if taxpayer_type == TaxpayerType.LZCHP:
            params["firstName"] = first_name
            params["lastName"] = last_name
        else:
            params["name"] = name
        
        # Заголовки запроса
        headers = {
            "X-Portal-Token": self.portal_token,
            "Accept": "application/json"
        }
        
        # Выполнение запроса
        try:
            response = requests.get(
                self.base_url,
                params=params,
                headers=headers,
                timeout=30
            )
            
            # Обработка ответа
            response.raise_for_status()
            
            # Проверяем content-type ответа
            content_type = response.headers.get('Content-Type', '').lower()
            
            if print:
                # Если запрошен PDF, проверяем что это действительно PDF
                if 'application/pdf' in content_type or 'pdf' in content_type:
                    # Если это PDF, возвращаем base64 строку
                    return {
                        "success": True,
                        "pdf_base64": response.text,
                        "content_type": "application/pdf"
                    }
                else:
                    # Если это не PDF (возможно, JSON ошибка), обрабатываем как JSON
                    try:
                        json_data = response.json()
                        return {
                            "success": False,
                            "error": "API вернул JSON вместо PDF",
                            "message": "Запрос PDF не выполнен. Получен JSON ответ.",
                            "data": json_data,
                            "status_code": response.status_code
                        }
                    except:
                        # Если не JSON, возвращаем как есть с предупреждением
                        return {
                            "success": False,
                            "error": "Неожиданный формат ответа",
                            "message": f"Ожидался PDF, получен: {content_type}",
                            "pdf_base64": response.text[:1000] if len(response.text) > 1000 else response.text
                        }
            else:
                # JSON ответ
                return {
                    "success": True,
                    "data": response.json(),
                    "status_code": response.status_code
                }
        
        except requests.exceptions.HTTPError as e:
            error_detail = {
                "success": False,
                "status_code": e.response.status_code,
                "error": self._get_error_message(e.response.status_code),
                "message": str(e)
            }
            
            # Попытка извлечь детали ошибки из ответа
            try:
                error_detail["details"] = e.response.json()
            except:
                error_detail["details"] = e.response.text
            
            return error_detail
        
        except requests.exceptions.RequestException as e:
            return {
                "success": False,
                "error": "Ошибка соединения с API",
                "message": str(e)
            }
    
    def _get_error_message(self, status_code: int) -> str:
        """Получить описание ошибки по коду статуса"""
        error_messages = {
            200: "Запрос выполнен успешно",
            400: "Запрос содержит синтаксическую ошибку",
            403: "Пользователь не авторизован",
            404: "Доступ к сервису запрещен",
            500: "Ошибка на сервере, запрос не выполнен"
        }
        return error_messages.get(status_code, f"Неизвестная ошибка (код {status_code})")
    
    def search_ip(
        self,
        taxpayer_code: str,
        name: str,
        print: bool = False
    ) -> Dict[str, Any]:
        """Поиск индивидуального предпринимателя"""
        return self.search_taxpayer(
            taxpayer_code=taxpayer_code,
            taxpayer_type=TaxpayerType.IP,
            name=name,
            print=print
        )
    
    def search_lzchp(
        self,
        taxpayer_code: str,
        first_name: str,
        last_name: str,
        print: bool = False
    ) -> Dict[str, Any]:
        """Поиск лица, занимающегося частной практикой"""
        return self.search_taxpayer(
            taxpayer_code=taxpayer_code,
            taxpayer_type=TaxpayerType.LZCHP,
            first_name=first_name,
            last_name=last_name,
            print=print
        )
    
    def search_ul(
        self,
        taxpayer_code: str,
        name: str,
        print: bool = False
    ) -> Dict[str, Any]:
        """Поиск юридического лица"""
        return self.search_taxpayer(
            taxpayer_code=taxpayer_code,
            taxpayer_type=TaxpayerType.UL,
            name=name,
            print=print
        )
