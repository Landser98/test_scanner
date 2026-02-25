from __future__ import annotations

from datetime import date, timedelta


from datetime import date
from typing import Tuple, Optional, List, Dict, Any

import pandas as pd

from .models import Statement
from .ip_config import IP_INCOME_CONFIG
from src.utils.income_calc import compute_ip_income

def _first_day_of_month(d: date) -> date:
    return d.replace(day=1)

def _add_months(d: date, months: int) -> date:
    y = d.year + (d.month - 1 + months) // 12
    m = (d.month - 1 + months) % 12 + 1
    return date(y, m, 1)

def get_last_full_12m_window(anchor: date) -> Tuple[date, date]:
    """
    Возвращает последние полные 12 календарных месяцев относительно anchor.

    Пример:
      anchor = 2024-06-27 -> окно 2023-07-01 .. 2024-06-30
    """
    anchor_month_start = _first_day_of_month(anchor)
    window_start = _add_months(anchor_month_start, -11)
    window_end = _add_months(anchor_month_start, 1) - timedelta(days=1)
    return window_start, window_end


def compute_ip_income_for_statement(
    stmnt: Statement,
    window_start: date,
    window_end: date,
) -> Tuple[Optional[pd.DataFrame], Optional[dict]]:
    """
    Returns:
      enriched_df (tx within window with ip_* flags) OR None
      summary dict OR None

    Important:
    - Works even if some configured columns (e.g. 'КНП') are missing in tx_df
      by creating them as empty strings.
    """
    cfg = IP_INCOME_CONFIG.get(stmnt.bank)
    if cfg is None:
        return None, None

    df = stmnt.tx_df.copy()
    if df is None or df.empty:
        return None, None

    # ---- helper: ensure required columns exist (avoid KeyError) ----
    def _ensure_col(colname: Optional[str], default_value: Any = "") -> None:
        if not colname:
            return
        if colname not in df.columns:
            df[colname] = default_value

    col_op_date = cfg.get("col_op_date")
    col_credit = cfg.get("col_credit")
    col_knp = cfg.get("col_knp")
    col_purpose = cfg.get("col_purpose")
    col_counterparty = cfg.get("col_counterparty")

    # Date + credit are essential for income calc
    if not col_op_date or col_op_date not in df.columns:
        raise ValueError(f"{stmnt.bank}/{stmnt.pdf_name}: missing date column '{col_op_date}'")

    if not col_credit or col_credit not in df.columns:
        raise ValueError(f"{stmnt.bank}/{stmnt.pdf_name}: missing credit column '{col_credit}'")

    # Optional columns: create if missing
    _ensure_col(col_knp, "")
    _ensure_col(col_purpose, "")
    _ensure_col(col_counterparty, "")

    # ---- normalize txn_date ----
    if "txn_date" not in df.columns:
        df["txn_date"] = pd.to_datetime(df[col_op_date], errors="coerce", dayfirst=True)
    else:
        if not pd.api.types.is_datetime64_any_dtype(df["txn_date"]):
            df["txn_date"] = pd.to_datetime(df["txn_date"], errors="coerce", dayfirst=True)

    df = df[df["txn_date"].notna()]
    if df.empty:
        return None, None

    # ---- apply 12m window ----
    df_win = df[
        (df["txn_date"] >= pd.Timestamp(window_start)) &
        (df["txn_date"] <= pd.Timestamp(window_end))
    ].copy()

    if df_win.empty:
        return None, None

    enriched, monthly_income, summary = compute_ip_income(
        df_win,
        col_op_date=col_op_date,
        col_credit=col_credit,
        col_knp=col_knp,
        col_purpose=col_purpose,
        col_counterparty=col_counterparty,
        months_back=None,               # window already applied here
        statement_generation_date=None, # don't re-filter inside compute_ip_income
        verbose=False,
    )

    # Attach metadata columns (consistent for UI/API)
    enriched["bank"] = stmnt.bank
    enriched["account_number"] = stmnt.account_number
    enriched["source_pdf"] = stmnt.pdf_name

    if summary is not None:
        summary = {
            "bank": stmnt.bank,
            "account_number": stmnt.account_number,
            "source_pdf": stmnt.pdf_name,
            **summary,
        }

    return enriched, summary


def build_metadata_df(statements: List[Statement]) -> pd.DataFrame:
    if not statements:
        return pd.DataFrame()

    rows = []
    for stmnt in statements:
        row = {
            "pdf_name": stmnt.pdf_name,
            "bank": stmnt.bank,
            "account_holder_name": stmnt.account_holder_name,
            "iin_bin": stmnt.iin_bin,
            "account_number": stmnt.account_number,
            "period_from": stmnt.period_from,
            "period_to": stmnt.period_to,
            "statement_generation_date": stmnt.statement_generation_date,
        }

        # Если у стейтмента есть header_df (например, BCC) — добавляем вкусняшки
        hdr_df = getattr(stmnt, "header_df", None)
        if hdr_df is not None and not hdr_df.empty:
            hdr = hdr_df.iloc[0]

            # поля из src/bcc/header.py
            mapping = {
                "Валюта": "currency",
                "БИК": "bic",
                "Кредитный лимит": "credit_limit",
                "Входящий остаток": "opening_balance",
                "Входящее сальдо": "incoming_saldo",
                "Реальный баланс": "real_balance",
                "Блокированные средства": "blocked_funds",
            }
            for src_col, out_col in mapping.items():
                if src_col in hdr.index:
                    row[out_col] = hdr[src_col]

        rows.append(row)

    return pd.DataFrame(rows)



def combine_transactions(statements: List[Statement], window_start: date, window_end: date, filter_by_date: bool = True) -> pd.DataFrame:
    """
    Объединяет транзакции из всех выписок.
    
    Args:
        statements: Список выписок
        window_start: Начало окна дат
        window_end: Конец окна дат
        filter_by_date: Если True, фильтрует транзакции по окну дат. Если False, возвращает все транзакции.
    
    Returns:
        DataFrame с объединенными транзакциями
    """
    if not statements:
        return pd.DataFrame()

    all_rows = []
    for stmnt in statements:
        df = stmnt.tx_df.copy()

        if "txn_date" not in df.columns:
            raise ValueError(f"{stmnt.bank}/{stmnt.pdf_name} missing txn_date")

        if not pd.api.types.is_datetime64_any_dtype(df["txn_date"]):
            df["txn_date"] = pd.to_datetime(df["txn_date"], errors="coerce")

        # Если фильтруем по датам, убираем транзакции без дат
        # Если не фильтруем, оставляем все транзакции (включая без дат)
        if filter_by_date:
            df = df[df["txn_date"].notna()]  # ✅ IMPORTANT - только если фильтруем по датам

        df["bank"] = stmnt.bank
        df["account_number"] = stmnt.account_number
        df["source_pdf"] = stmnt.pdf_name
        all_rows.append(df)

    all_tx = pd.concat(all_rows, ignore_index=True)

    if filter_by_date:
        mask = (all_tx["txn_date"] >= pd.Timestamp(window_start)) & (all_tx["txn_date"] <= pd.Timestamp(window_end))
        filtered = all_tx.loc[mask].copy()
    else:
        # Не фильтруем по датам - возвращаем все транзакции
        filtered = all_tx.copy()

    # optional dedup
    # Если фильтруем по датам, используем txn_date для дедупликации
    # Если не фильтруем, используем только bank и account_number (так как могут быть транзакции без дат)
    if filter_by_date:
        dedup_subset = [c for c in ["bank", "account_number", "txn_date"] if c in filtered.columns]
    else:
        dedup_subset = [c for c in ["bank", "account_number"] if c in filtered.columns]
        # Если есть txn_date, добавляем его для более точной дедупликации
        if "txn_date" in filtered.columns:
            dedup_subset.append("txn_date")
    
    if dedup_subset:
        filtered = filtered.drop_duplicates(subset=dedup_subset)

    return filtered


def build_metadata_df(statements: List[Statement]) -> pd.DataFrame:
    if not statements:
        return pd.DataFrame()

    rows = []
    for stmnt in statements:
        row = {
            "pdf_name": stmnt.pdf_name,
            "bank": stmnt.bank,
            "account_holder_name": stmnt.account_holder_name,
            "iin_bin": stmnt.iin_bin,
            "account_number": stmnt.account_number,
            "period_from": stmnt.period_from,
            "period_to": stmnt.period_to,
            "statement_generation_date": stmnt.statement_generation_date,
        }

        # Если у стейтмента есть header_df (например, BCC) — добавляем вкусняшки
        hdr_df = getattr(stmnt, "header_df", None)
        if hdr_df is not None and not hdr_df.empty:
            hdr = hdr_df.iloc[0]

            # поля из src/bcc/header.py
            mapping = {
                "Валюта": "currency",
                "БИК": "bic",
                "Кредитный лимит": "credit_limit",
                "Входящий остаток": "opening_balance",
                "Входящее сальдо": "incoming_saldo",
                "Реальный баланс": "real_balance",
                "Блокированные средства": "blocked_funds",
            }
            for src_col, out_col in mapping.items():
                if src_col in hdr.index:
                    row[out_col] = hdr[src_col]

        rows.append(row)

    return pd.DataFrame(rows)

