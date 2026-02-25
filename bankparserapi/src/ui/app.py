#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
src/ui/app.py

Streamlit UI for bank statement parsing + 12m window analysis.
"""
from __future__ import annotations

from pathlib import Path
import sys
from datetime import date
from typing import List, Optional, Dict, Any
import re

import pandas as pd
import streamlit as st

# --- ensure project root on sys.path ---
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# -----------------------------
# Helpers
# -----------------------------
def init_session_state() -> None:
    if "client_name" not in st.session_state:
        st.session_state.client_name = ""
    if "anchor_date" not in st.session_state:
        st.session_state.anchor_date = date.today()
    if "statements" not in st.session_state:
        st.session_state.statements = []
    if "session_iin_bin" not in st.session_state:
        st.session_state.session_iin_bin = None
    if "allow_iin_mismatch" not in st.session_state:
        st.session_state.allow_iin_mismatch = False


def _format_bank_label(bank_key: str) -> str:
    return {
        "kaspi_gold": "Kaspi Gold",
        "kaspi_pay": "Kaspi Pay",
        "halyk_business": "Halyk (Business)",
        "halyk_individual": "Halyk (Individual)",
        "freedom_bank": "Freedom Bank",
        "forte_bank": "ForteBank",
        "eurasian_bank": "Eurasian Bank",
        "bcc_bank": "BCC (CenterCredit)",
        "alatau_city_bank": "Alatau City Bank",
    }.get(bank_key, bank_key)


def ensure_txn_date(statement) -> None:
    df = getattr(statement, "tx_df", None)
    if df is None or df.empty:
        return

    if "txn_date" in df.columns:
        if not pd.api.types.is_datetime64_any_dtype(df["txn_date"]):
            df["txn_date"] = pd.to_datetime(df["txn_date"], errors="coerce", dayfirst=True)
    else:
        from src.core.ip_config import IP_INCOME_CONFIG
        cfg = IP_INCOME_CONFIG.get(getattr(statement, "bank", ""), {})
        date_col = cfg.get("col_op_date")
        candidates = [date_col, "Дата", "date", "Дата операции", "Дата проводки", "txn_date"]
        candidates = [c for c in candidates if c and c in df.columns]
        if candidates:
            df["txn_date"] = pd.to_datetime(df[candidates[0]], errors="coerce", dayfirst=True)
        else:
            # Если не нашли колонку с датой, создаем txn_date с сегодняшней датой
            df["txn_date"] = pd.Timestamp(date.today())
    
    # Убеждаемся, что колонка txn_date существует и имеет правильный тип
    statement.tx_df = df


def build_metadata_df(statements) -> pd.DataFrame:
    if not statements: return pd.DataFrame()
    rows = []
    for s in statements:
        rows.append({
            "Имя файла": s.pdf_name,
            "Банк": s.bank,
            "ИИН/БИН": s.iin_bin,
            "Период": f"{s.period_from} - {s.period_to}",
        })
    return pd.DataFrame(rows)


# -----------------------------
# Main App
# -----------------------------
def main() -> None:
    st.set_page_config(page_title="Анализатор банковских выписок", layout="wide")
    st.title("Анализатор банковских выписок")

    init_session_state()

    # --- Step 1: Meta ---
    with st.form("client_form"):
        st.session_state.client_name = st.text_input("Имя клиента", value=st.session_state.client_name)
        st.session_state.anchor_date = st.date_input("Дата для расчета", value=st.session_state.anchor_date)
        if st.form_submit_button("Обновить сессию"):
            st.success("Сессия обновлена")

    from src.core.analysis import get_last_full_12m_window
    window_start, window_end = get_last_full_12m_window(st.session_state.anchor_date)
    st.info(f"Окно анализа: **{window_start}** → **{window_end}** (год начиная с выбранной даты)")

    # --- Step 2: Upload ---
    st.header("2. Загрузка выписок")
    col_bank, col_file = st.columns([1, 3])
    with col_bank:
        bank_key = st.selectbox("Банк", options=["kaspi_gold", "kaspi_pay", "halyk_business", "halyk_individual",
                                                 "freedom_bank", "forte_bank", "eurasian_bank", "bcc_bank",
                                                 "alatau_city_bank"], format_func=_format_bank_label)
    with col_file:
        uploaded_file = st.file_uploader("Загрузить PDF", type=["pdf"])

    if uploaded_file and st.button("Парсить и добавить выписку", type="primary"):
        try:
            from src.core.service import parse_statement
            stmnt = parse_statement(bank_key=bank_key, pdf_name=uploaded_file.name, pdf_bytes=uploaded_file.read())
            ensure_txn_date(stmnt)
            st.session_state.statements.append(stmnt)
            st.success(f"Добавлено: {uploaded_file.name}")
        except Exception as e:
            st.exception(e)

    if not st.session_state.statements:
        return

    st.subheader("Загруженные выписки")
    st.dataframe(build_metadata_df(st.session_state.statements), use_container_width=True)

    # --- Step 4: UI Analysis Tables ---
    st.header("4. Аналитические таблицы (Топ-9 и Аффилированные лица)")
    from src.core.analysis import combine_transactions
    
    # Чекбокс для выбора - учитывать даты или нет
    if "filter_by_date" not in st.session_state:
        st.session_state.filter_by_date = False
    
    filter_by_date = st.checkbox(
        "Фильтровать транзакции по датам",
        value=st.session_state.filter_by_date,
        help="Если включено, учитываются только транзакции в указанном диапазоне дат. Если выключено, учитываются все транзакции."
    )
    st.session_state.filter_by_date = filter_by_date
    
    # Определяем окно дат (используется только если filter_by_date = True)
    # Для теста: год начиная с anchor_date
    window_start, window_end = get_last_full_12m_window(st.session_state.anchor_date)
    
    if filter_by_date:
        st.info(f"📅 Фильтрация по датам включена. Окно: {window_start} → {window_end}")
    else:
        st.info("📅 Фильтрация по датам выключена. Учитываются все транзакции.")
    
    tx_12m = combine_transactions(st.session_state.statements, window_start, window_end, filter_by_date=filter_by_date)

    if not tx_12m.empty:
        from src.ui.ui_analysis_report_generator import get_ui_analysis_tables
        # Работаем с копией для аналитики, чтобы не портить tx_12m для отображения в конце
        df_analysis = tx_12m.copy()

        # 1. ЧИСТКА СУММ (обработка запятых и спец-пробелов)
        def clean_amt_val(v):
            if pd.isna(v) or v == '': return 0.0
            if isinstance(v, (int, float)): return float(v)
            s = str(v).replace(',', '').replace(' ', '').replace('\xa0', '').replace('\u00A0', '').strip()
            try:
                return float(s)
            except:
                return 0.0

        # 2. ОПРЕДЕЛЕНИЕ СУММЫ (amount)
        # Если есть Дебет и Кредит (Halyk Business / Kaspi Pay)
        if 'Дебет' in df_analysis.columns and 'Кредит' in df_analysis.columns:
            d_clean = df_analysis['Дебет'].apply(clean_amt_val)
            k_clean = df_analysis['Кредит'].apply(clean_amt_val)
            df_analysis['amount'] = k_clean - d_clean
        elif 'amount' not in df_analysis.columns:
            amt_col = next((c for c in ['Сумма операции', 'Сумма', 'Расход', 'Кредит'] if c in df_analysis.columns),
                           None)
            if amt_col:
                df_analysis['amount'] = df_analysis[amt_col].apply(clean_amt_val)
            else:
                df_analysis['amount'] = 0.0

        # 3. ОПРЕДЕЛЕНИЕ ОПИСАНИЯ
        desc_col = next(
            (c for c in ['Детали платежа', 'Описание операции', 'details', 'Назначение платежа', 'operation'] if
             c in df_analysis.columns), None)
        df_analysis['details'] = df_analysis[desc_col].fillna('') if desc_col else ''

        # 4. ОПРЕДЕЛЕНИЕ КОНТРАГЕНТА (counterparty_id = БИН)
        def get_cp_data(row):
            # Список колонок, где может быть имя/БИН контрагента
            cp_candidates = ['Контрагент', 'Контрагент (имя)', 'Корреспондент', 'Наименование получателя']
            cp_text = ""
            for col in cp_candidates:
                if col in row and pd.notna(row[col]):
                    cp_text = str(row[col])
                    break

            # Ищем БИН (12 цифр подряд)
            bin_match = re.search(r'(\d{12})', cp_text)
            if bin_match:
                bin_val = bin_match.group(1)
                # Имя: берем текст до слова БИН или первую строку
                name = cp_text.split('БИН')[0].split('ИИН')[0].split('\n')[0].strip()
                return bin_val, (name if name else bin_val)

            # Если БИН не найден
            name_fallback = cp_text.split('\n')[0].strip() if cp_text else (row.get('details') or 'Н/Д')
            return str(name_fallback), str(name_fallback)

        cp_results = df_analysis.apply(get_cp_data, axis=1)
        df_analysis['counterparty_id'] = [x[0] for x in cp_results]
        df_analysis['counterparty_name'] = [x[1] for x in cp_results]

        # ГЕНЕРАЦИЯ ТАБЛИЦ
        analysis = get_ui_analysis_tables(df_analysis)

        # ОТОБРАЖЕНИЕ ТАБЛИЦ
        c1, c2 = st.columns(2)
        with c1:
            st.write("**Расходы (Дебет)**")
            if analysis["debit_top"]:
                st.dataframe(pd.DataFrame(analysis["debit_top"]), use_container_width=True, hide_index=True)
            else:
                st.info("Нет данных по расходам")
        with c2:
            st.write("**Приходы (Кредит)**")
            if analysis["credit_top"]:
                st.dataframe(pd.DataFrame(analysis["credit_top"]), use_container_width=True, hide_index=True)
            else:
                st.info("Нет данных по приходам")

        st.subheader("Аффилированные лица (Net расчет)")
        if analysis["related_parties"]:
            rp_df = pd.DataFrame(analysis["related_parties"])
            st.dataframe(rp_df.sort_values("Оборот", ascending=False), use_container_width=True, hide_index=True)

    # --- Step 5: Enriched Transactions ---
    st.header("5. Транзакции с флагами IP (Анализ дохода ИП)")
    enriched_list = []
    for s in st.session_state.statements:
        from src.core.analysis import compute_ip_income_for_statement
        df_en, _ = compute_ip_income_for_statement(s, window_start, window_end)
        if df_en is not None:
            enriched_list.append(df_en)

    if enriched_list:
        all_enriched = pd.concat(enriched_list, ignore_index=True)
        
        # Переименование колонок на русский
        column_mapping = {
            'bank': 'Банк',
            'account_number': 'Номер счета',
            'source_pdf': 'Источник PDF',
            'ip_knp_norm': 'КНП (норм)',
            'ip_op_date': 'Дата операции (IP)',
            'ip_is_non_business_by_knp': 'Не бизнес (по КНП)',
            'ip_is_non_business_by_keywords': 'Не бизнес (по ключевым словам)',
            'ip_is_non_business': 'Не бизнес',
            'ip_is_business_income': 'Бизнес-доход',
            'ip_credit_amount': 'Сумма бизнес-дохода',
            'txn_date': 'Дата транзакции'
        }
        
        # Переименовываем только существующие колонки
        display_df = all_enriched.rename(columns={k: v for k, v in column_mapping.items() if k in all_enriched.columns})
        st.dataframe(display_df, use_container_width=True)
    else:
        st.info("Транзакции бизнес-дохода не найдены.")

    with st.expander("Все транзакции"):
        # Здесь показываем оригинальный tx_12m, чтобы видеть все колонки из PDF
        # Переименовываем стандартные колонки на русский, если они есть
        tx_display = tx_12m.copy()
        tx_column_mapping = {
            'bank': 'Банк',
            'account_number': 'Номер счета',
            'source_pdf': 'Источник PDF',
            'txn_date': 'Дата транзакции'
        }
        tx_display = tx_display.rename(columns={k: v for k, v in tx_column_mapping.items() if k in tx_display.columns})
        st.dataframe(tx_display, use_container_width=True)


if __name__ == "__main__":
    main()