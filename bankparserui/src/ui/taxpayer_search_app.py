#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
UI для поиска налогоплательщика через API сервиса «Поиск Налогоплательщика».
"""

from __future__ import annotations

from pathlib import Path
import sys
from typing import Optional, Dict, Any
import base64
import json

import streamlit as st
import requests

# --- ensure project root on sys.path ---
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.api.taxpayer_api import TaxpayerAPIClient, TaxpayerType


# Константы
# SECURITY: Token from env only, never hardcoded; set TAXPAYER_API_PORTAL_TOKEN in production
import os
DEFAULT_PORTAL_TOKEN = os.environ.get("TAXPAYER_API_PORTAL_TOKEN", "")


def init_session_state() -> None:
    """Инициализация переменных сессии"""
    if "taxpayer_search_results" not in st.session_state:
        st.session_state.taxpayer_search_results = []
    if "portal_host" not in st.session_state:
        st.session_state.portal_host = ""
    if "portal_token" not in st.session_state:
        st.session_state.portal_token = DEFAULT_PORTAL_TOKEN


def format_taxpayer_response(data: Dict[str, Any]) -> str:
    """Форматирование ответа API для отображения"""
    if not data:
        return "Нет данных"
    
    responses = data.get("taxpayerPortalSearchResponses", [])
    if not responses:
        return "Нет результатов"
    
    formatted = []
    for resp in responses:
        result = []
        result.append(f"**UID сообщения:** {resp.get('responseMessageUid', 'N/A')}")
        result.append(f"**Результат:** {resp.get('messageResult', 'N/A')}")
        result.append(f"**Код:** {resp.get('code', 'N/A')}")
        result.append(f"**Тип:** {resp.get('taxpayerType', 'N/A')}")
        
        if resp.get('name'):
            result.append(f"**Наименование:** {resp['name']}")
        
        if resp.get('fullName'):
            full_name = resp['fullName']
            name_parts = []
            if full_name.get('lastName'):
                name_parts.append(full_name['lastName'])
            if full_name.get('firstName'):
                name_parts.append(full_name['firstName'])
            if full_name.get('middleName'):
                name_parts.append(full_name['middleName'])
            if name_parts:
                result.append(f"**ФИО:** {' '.join(name_parts)}")
        
        if resp.get('beginDate'):
            result.append(f"**Дата начала:** {resp['beginDate']}")
        
        if resp.get('endDate'):
            result.append(f"**Дата окончания:** {resp['endDate']}")
        
        if resp.get('endReason'):
            end_reason = resp['endReason']
            result.append(f"**Причина окончания:** {end_reason.get('ru', end_reason.get('code', 'N/A'))}")
        
        if resp.get('lzchpTypes'):
            result.append("**Типы ЛЗЧП:**")
            for lzchp_type in resp['lzchpTypes']:
                result.append(f"  - {lzchp_type.get('lzchpType', 'N/A')} "
                             f"(с {lzchp_type.get('beginDate', 'N/A')} "
                             f"по {lzchp_type.get('endDate', 'N/A') or 'настоящее время'})")
        
        formatted.append("\n".join(result))
    
    return "\n\n---\n\n".join(formatted)


def display_pdf_result(pdf_base64: str):
    """Отображение PDF результата"""
    try:
        pdf_bytes = base64.b64decode(pdf_base64)
        st.download_button(
            label="📥 Скачать PDF",
            data=pdf_bytes,
            file_name="taxpayer_search_result.pdf",
            mime="application/pdf"
        )
        
        # Попытка отобразить PDF встроенным способом
        st.markdown("### Предпросмотр PDF")
        st.markdown(
            f'<iframe src="data:application/pdf;base64,{pdf_base64}" '
            f'width="100%" height="600px" type="application/pdf"></iframe>',
            unsafe_allow_html=True
        )
    except Exception as e:
        st.error(f"Ошибка при обработке PDF: {str(e)}")


def main() -> None:
    """Основная функция приложения"""
    st.set_page_config(
        page_title="Поиск Налогоплательщика",
        page_icon="🔍",
        layout="wide"
    )
    
    st.title("🔍 Поиск Налогоплательщика")
    st.markdown("""
    **Поиск информации о налогоплательщике через API сервиса «Поиск Налогоплательщика».**
    
    Поддерживаются следующие типы налогоплательщиков:
    - **ИП** (Индивидуальный предприниматель)
    - **ЛЗЧП** (Лицо, занимающееся частной практикой)
    - **ЮЛ** (Юридическое лицо)
    """)
    
    init_session_state()
    
    # Настройки в боковой панели
    with st.sidebar:
        st.header("⚙️ Настройки API")
        
        portal_host = st.text_input(
            "🌐 Portal Host",
            value=st.session_state.portal_host,
            placeholder="https://portal.example.com",
            help="Базовый URL портала API"
        )
        st.session_state.portal_host = portal_host
        
        portal_token = st.text_input(
            "🔑 X-Portal-Token",
            value=st.session_state.portal_token,
            type="password",
            help="Токен доступа к порталу"
        )
        st.session_state.portal_token = portal_token
        
        if st.button("🔄 Сбросить настройки"):
            st.session_state.portal_host = ""
            st.session_state.portal_token = DEFAULT_PORTAL_TOKEN
            st.rerun()
    
    # Основная форма поиска
    st.header("📝 Форма поиска")
    
    with st.form("taxpayer_search_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            taxpayer_type = st.selectbox(
                "Тип налогоплательщика *",
                options=["IP", "LZCHP", "UL"],
                help="Выберите тип налогоплательщика"
            )
            
            taxpayer_code = st.text_input(
                "ИИН/БИН *",
                placeholder="444444444444",
                help="12-значный ИИН или БИН налогоплательщика",
                max_chars=12
            )
        
        with col2:
            # Поля в зависимости от типа налогоплательщика
            if taxpayer_type == "LZCHP":
                first_name = st.text_input(
                    "Имя *",
                    placeholder="First",
                    help="Имя для ЛЗЧП"
                )
                last_name = st.text_input(
                    "Фамилия *",
                    placeholder="Last",
                    help="Фамилия для ЛЗЧП"
                )
                name = None
            else:
                name = st.text_input(
                    "Наименование *",
                    placeholder="TOO",
                    help="Наименование для ИП или ЮЛ"
                )
                first_name = None
                last_name = None
        
        # Дополнительные опции
        return_pdf = st.checkbox(
            "Вернуть результат в виде PDF",
            value=False,
            help="Если отмечено, результат будет возвращен как PDF документ в base64"
        )
        
        submitted = st.form_submit_button("🔍 Найти", type="primary")
    
    # Обработка формы
    if submitted:
        # Валидация
        if not st.session_state.portal_host:
            st.error("❌ Укажите Portal Host в настройках")
            return
        
        if not st.session_state.portal_token:
            st.error("❌ Укажите X-Portal-Token в настройках")
            return
        
        if not taxpayer_code or len(taxpayer_code) != 12 or not taxpayer_code.isdigit():
            st.error("❌ ИИН/БИН должен быть строкой из 12 цифр")
            return
        
        if taxpayer_type == "LZCHP":
            if not first_name or not last_name:
                st.error("❌ Для ЛЗЧП необходимо указать имя и фамилию")
                return
        else:
            if not name:
                st.error("❌ Для ИП и ЮЛ необходимо указать наименование")
                return
        
        # Выполнение поиска
        with st.spinner("🔍 Выполняется поиск..."):
            try:
                client = TaxpayerAPIClient(
                    portal_host=st.session_state.portal_host,
                    portal_token=st.session_state.portal_token
                )
                
                taxpayer_type_enum = TaxpayerType[taxpayer_type]
                
                result = client.search_taxpayer(
                    taxpayer_code=taxpayer_code,
                    taxpayer_type=taxpayer_type_enum,
                    name=name,
                    first_name=first_name,
                    last_name=last_name,
                    print=return_pdf
                )
                
                # Сохранение результата в сессии
                search_record = {
                    "taxpayer_code": taxpayer_code,
                    "taxpayer_type": taxpayer_type,
                    "result": result,
                    "timestamp": st.session_state.get("timestamp", "")
                }
                st.session_state.taxpayer_search_results.insert(0, search_record)
                
                # Отображение результата
                st.success("✅ Поиск выполнен!")
                
                if result.get("success"):
                    if return_pdf and result.get("pdf_base64"):
                        st.subheader("📄 Результат поиска (PDF)")
                        display_pdf_result(result["pdf_base64"])
                    else:
                        st.subheader("📊 Результат поиска")
                        data = result.get("data", {})
                        
                        # Отображение в виде JSON
                        with st.expander("📋 JSON ответ", expanded=True):
                            st.json(data)
                        
                        # Отображение в читаемом формате
                        formatted = format_taxpayer_response(data)
                        if formatted:
                            st.markdown("### 📝 Форматированный результат")
                            st.markdown(formatted)
                else:
                    st.error(f"❌ Ошибка поиска: {result.get('error', 'Неизвестная ошибка')}")
                    if result.get("message"):
                        st.error(f"Детали: {result['message']}")
                    if result.get("status_code"):
                        st.info(f"Код статуса: {result['status_code']}")
                    if result.get("data"):
                        with st.expander("Детали ошибки"):
                            st.json(result["data"])
            
            except Exception as e:
                st.error(f"❌ Критическая ошибка: {str(e)}")
                st.exception(e)
    
    # История поисков
    if st.session_state.taxpayer_search_results:
        st.divider()
        st.header("📜 История поисков")
        
        for idx, record in enumerate(st.session_state.taxpayer_search_results[:10]):  # Показываем последние 10
            with st.expander(
                f"🔍 {record['taxpayer_type']} - {record['taxpayer_code']} "
                f"({'✅ Успех' if record['result'].get('success') else '❌ Ошибка'})"
            ):
                result = record["result"]
                
                if result.get("success"):
                    if result.get("pdf_base64"):
                        st.info("Результат: PDF документ")
                        display_pdf_result(result["pdf_base64"])
                    else:
                        data = result.get("data", {})
                        st.json(data)
                        formatted = format_taxpayer_response(data)
                        if formatted:
                            st.markdown(formatted)
                else:
                    st.error(f"Ошибка: {result.get('error', 'Неизвестная ошибка')}")
                    if result.get("message"):
                        st.error(f"Детали: {result['message']}")
        
        if st.button("🗑️ Очистить историю"):
            st.session_state.taxpayer_search_results = []
            st.rerun()


if __name__ == "__main__":
    main()
