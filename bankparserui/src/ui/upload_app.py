#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
New UI for uploading bank statements with automatic bank detection.
Handles multiple statements from different banks and saves to database.
"""

from __future__ import annotations

from pathlib import Path
import sys
from datetime import date
from typing import List, Dict, Any, Optional
import uuid
import base64

import pandas as pd
import streamlit as st
import requests

# --- ensure project root on sys.path ---
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.api.statement_processor import StatementProcessor
from src.core.analysis import get_last_full_12m_window, compute_ip_income_for_statement, combine_transactions
from src.db.database import DatabaseConnection, import_statement_to_db
from src.db.config import DB_CONFIG
from src.ui.ui_analysis_report_generator import get_ui_analysis_tables
from src.api.storage import get_storage
from src.api.taxpayer_api import TaxpayerAPIClient, TaxpayerType
from datetime import datetime

# ==================== Константы для API поиска налогоплательщика ====================
# Настройте эти значения для подключения к API «Поиск Налогоплательщика»
# Получите реальный URL портала у администратора КГД МФ РК
# SECURITY: Use environment variables instead of hardcoded values
import os
TAXPAYER_API_PORTAL_HOST = os.environ.get(
    "TAXPAYER_API_PORTAL_HOST",
    ""
)
API_BASE_URL = os.environ.get("API_BASE_URL", "http://127.0.0.1:8000")


def check_api_health() -> tuple[bool, str]:
    """Check API availability for inter-service connectivity diagnostics."""
    try:
        response = requests.get(f"{API_BASE_URL.rstrip('/')}/livez", timeout=3)
        if response.status_code == 200:
            return True, "API доступен"
        return False, f"API вернул статус {response.status_code}"
    except Exception as exc:
        return False, f"API недоступен: {exc}"


def init_session_state() -> None:
    """Initialize session state variables"""
    if "upload_results" not in st.session_state:
        st.session_state.upload_results = []
    if "processed_statements" not in st.session_state:
        st.session_state.processed_statements = []
    if "projects_created" not in st.session_state:
        st.session_state.projects_created = []
    if "anchor_date" not in st.session_state:
        st.session_state.anchor_date = date.today()
    # Taxpayer search state
    if "taxpayer_search_results" not in st.session_state:
        st.session_state.taxpayer_search_results = []
    if "selected_project_id" not in st.session_state:
        st.session_state.selected_project_id = None


def format_bank_name(bank_key: str) -> str:
    """Format bank key to readable name"""
    bank_names = {
        "kaspi_gold": "Kaspi Gold",
        "kaspi_pay": "Kaspi Pay",
        "halyk_business": "Halyk Business",
        "halyk_individual": "Halyk Individual",
        "freedom_bank": "Freedom Bank",
        "forte_bank": "Forte Bank",
        "eurasian_bank": "Eurasian Bank",
        "bcc_bank": "BCC Bank",
        "alatau_city_bank": "Alatau City Bank",
    }
    return bank_names.get(bank_key, bank_key)


def _ensure_project_schema() -> None:
    db = DatabaseConnection(**DB_CONFIG)
    db.connect()
    try:
        db.ensure_project_schema()
    finally:
        db.disconnect()


def _create_project(name: str, created_by: str = "streamlit_8502") -> str:
    db = DatabaseConnection(**DB_CONFIG)
    db.connect()
    try:
        project_id = db.execute_insert(
            """
            INSERT INTO projects (name, status, created_by)
            VALUES (%s, 'draft', %s)
            RETURNING id
            """,
            (name.strip(), created_by),
        )
        return str(project_id)
    finally:
        db.disconnect()


def _list_projects() -> List[Dict[str, Any]]:
    db = DatabaseConnection(**DB_CONFIG)
    db.connect()
    try:
        rows = db.execute_query(
            """
            SELECT
                p.id,
                p.name,
                p.status,
                p.created_at,
                COUNT(ps.id) AS statements_count
            FROM projects p
            LEFT JOIN project_statements ps ON ps.project_id = p.id
            GROUP BY p.id, p.name, p.status, p.created_at
            ORDER BY p.created_at DESC
            """
        )
        return rows
    finally:
        db.disconnect()


def _count_project_statements(project_id: str) -> int:
    db = DatabaseConnection(**DB_CONFIG)
    db.connect()
    try:
        rows = db.execute_query(
            "SELECT COUNT(*) AS cnt FROM project_statements WHERE project_id = %s",
            (project_id,),
        )
        return int(rows[0]["cnt"]) if rows else 0
    finally:
        db.disconnect()


def _link_statement_to_project(
    project_id: str,
    statement_id: Optional[str],
    upload_order: int,
    source_filename: str,
    processing_status: str,
    processing_message: str,
) -> None:
    db = DatabaseConnection(**DB_CONFIG)
    db.connect()
    try:
        db.execute_insert(
            """
            INSERT INTO project_statements (
                project_id, statement_id, upload_order, source_filename, processing_status, processing_message
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (project_id, statement_id, upload_order, source_filename, processing_status, processing_message),
        )
    finally:
        db.disconnect()


def _update_project_status(project_id: str, status: str) -> None:
    db = DatabaseConnection(**DB_CONFIG)
    db.connect()
    try:
        db.execute_command(
            "UPDATE projects SET status = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s",
            (status, project_id),
        )
    finally:
        db.disconnect()


def _resolve_income_anchor_date(parsed_statement, fallback: Optional[date] = None) -> date:
    """
    Priority:
      1) statement_generation_date
      2) period_to
      3) fallback (UI selected date)
      4) today
    """
    for attr_name in ("statement_generation_date", "period_to"):
        raw_value = getattr(parsed_statement, attr_name, None)
        if raw_value is None:
            continue
        if isinstance(raw_value, datetime):
            return raw_value.date()
        if isinstance(raw_value, pd.Timestamp):
            return raw_value.date()
        if isinstance(raw_value, date):
            return raw_value
        parsed = pd.to_datetime(raw_value, errors="coerce")
        if pd.notna(parsed):
            return parsed.date()

    return fallback if fallback else date.today()


def _build_monthly_ip_income_df(enriched_df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    """
    Build monthly IP income from rows marked as business income.
    Mirrors monthly table semantics from *_ip_income_monthly.csv.
    """
    if enriched_df is None or enriched_df.empty:
        return None
    if "txn_date" not in enriched_df.columns or "ip_credit_amount" not in enriched_df.columns:
        return None

    work_df = enriched_df.copy()
    work_df["txn_date"] = pd.to_datetime(work_df["txn_date"], errors="coerce")
    work_df = work_df[work_df["txn_date"].notna()]

    if "ip_is_business_income" in work_df.columns:
        work_df = work_df[work_df["ip_is_business_income"].fillna(False).astype(bool)].copy()
    else:
        work_df = work_df[work_df["ip_credit_amount"].fillna(0.0) > 0].copy()

    if work_df.empty:
        return pd.DataFrame(columns=["month", "business_income"])

    work_df["month"] = work_df["txn_date"].dt.to_period("M").astype(str)
    monthly_summary = (
        work_df.groupby("month", as_index=False)
        .agg(
            business_income=("ip_credit_amount", "sum"),
            transaction_count=("ip_credit_amount", "count"),
        )
    )
    return monthly_summary


def set_transaction_dates_to_today(statement, target_date: Optional[date] = None) -> None:
    """
    Устанавливает все даты транзакций на указанную дату (по умолчанию сегодня) для тестирования.
    Это позволяет транзакциям попадать в 12-месячное окно.
    """
    if not hasattr(statement, "tx_df") or statement.tx_df is None or statement.tx_df.empty:
        return
    
    # Работаем напрямую с DataFrame, чтобы изменения сохранились
    df = statement.tx_df
    target = target_date if target_date else date.today()
    target_ts = pd.Timestamp(target)
    
    # Устанавливаем даты для всех строк
    if "txn_date" not in df.columns:
        # Если колонки txn_date нет, создаем её
        df["txn_date"] = target_ts
    else:
        # Устанавливаем все даты на целевую дату для всех строк
        # Используем .loc для гарантированного изменения всех значений
        df.loc[:, "txn_date"] = target_ts
    
    # Убеждаемся, что тип данных правильный
    if not pd.api.types.is_datetime64_any_dtype(df["txn_date"]):
        df["txn_date"] = pd.to_datetime(df["txn_date"], errors="coerce")
    
    # Убеждаемся, что изменения применены
    statement.tx_df = df


def process_statements_like_upload_initial(
    uploaded_files: List,
    processor: StatementProcessor,
    anchor_date: Optional[date] = None
) -> Dict[str, Any]:
    """
    Process statements similar to upload_initial API endpoint.
    Groups statements by IIN and creates projects.
    """
    storage = get_storage()
    all_results = []
    projects_created = []
    
    # Step 1: Parse all statements
    statements_data = []  # (statement_id, pdf_bytes, filename, parse_result)
    parsed_statements_by_iin = {}  # {iin: [parsed_statements]}
    
    for uploaded_file in uploaded_files:
        statement_id = str(uuid.uuid4())
        pdf_bytes = uploaded_file.read()
        base64_data = base64.b64encode(pdf_bytes).decode('utf-8')
        extension = ".pdf" if uploaded_file.name.lower().endswith('.pdf') else ""
        
        # Parse statement
        parse_result = processor.parse_statement_base64(
            statement_id=statement_id,
            statement_name=uploaded_file.name,
            extension=extension,
            base64_data=base64_data,
            expected_iin=None  # No IIN validation
        )
        
        statements_data.append((statement_id, pdf_bytes, uploaded_file.name, parse_result))
        
        # Extract IIN from parsed statement
        parsed_statement = parse_result.get("parsed_statement")
        if parsed_statement:
            # Используем реальные даты транзакций из выписок
            pass
            
            iin = getattr(parsed_statement, "iin_bin", None)
            if iin:
                iin = iin.strip()
                if iin not in parsed_statements_by_iin:
                    parsed_statements_by_iin[iin] = []
                parsed_statements_by_iin[iin].append({
                    "statement_id": statement_id,
                    "parsed_statement": parsed_statement,
                    "parse_result": parse_result,
                    "pdf_bytes": pdf_bytes,
                    "filename": uploaded_file.name
                })
    
    # Step 2: Process each IIN group (like upload_initial)
    for iin, statements_group in parsed_statements_by_iin.items():
        statements_resp = []
        parsed_statements = []
        statement_files_data = []
        has_data_mismatch = False
        has_failure = False
        
        # Process each statement in the group
        for stmt_data in statements_group:
            parse_result = stmt_data["parse_result"]
            parsed_statement = stmt_data["parsed_statement"]
            statement_id = stmt_data["statement_id"]
            pdf_bytes = stmt_data["pdf_bytes"]
            filename = stmt_data["filename"]
            
            # Track status
            status = parse_result.get("status")
            if status == processor.STATUS_DATA_MISMATCH:
                has_data_mismatch = True
            elif status == processor.STATUS_FAILURE or status == processor.STATUS_SCANNED_COPY:
                has_failure = True
            
            # Store file data
            ext = ".pdf" if filename.lower().endswith('.pdf') else ""
            if ext and not filename.endswith(ext):
                filename = f"{filename}{ext}"
            statement_files_data.append((statement_id, pdf_bytes, filename))
            
            # Add to response
            statements_resp.append({
                'id': statement_id,
                'name': filename,
                'extension': ext,
                'status': status,
                'message': parse_result.get("message", "")
            })
            
            # Process and save to DB if successful
            if status == processor.STATUS_SUCCESS:
                try:
                    # Calculate income in the same way as batch parsers:
                    # anchor by statement date, fallback to UI-selected date.
                    calc_date = _resolve_income_anchor_date(parsed_statement, fallback=anchor_date)
                    window_start, window_end = get_last_full_12m_window(calc_date)
                    
                    enriched_df, income_summary = compute_ip_income_for_statement(
                        parsed_statement,
                        window_start,
                        window_end
                    )
                    
                    # Prepare monthly income DataFrame
                    monthly_income_df = _build_monthly_ip_income_df(enriched_df)
                    
                    # Save to database
                    bank_name = format_bank_name(getattr(parsed_statement, "bank", "Неизвестно"))
                    statement_data = {
                        'header_df': getattr(parsed_statement, 'header_df', None),
                        'tx_df': getattr(parsed_statement, 'tx_df', None),
                        'footer_df': getattr(parsed_statement, 'footer_df', None),
                        'meta_df': getattr(parsed_statement, 'meta_df', None),
                        'tx_ip_df': enriched_df,
                        'monthly_income_df': monthly_income_df,
                        'income_summary': income_summary if income_summary else {},
                        'client_iin': iin,  # ИИН извлекается из выписки
                        'client_name': getattr(parsed_statement, 'account_holder_name', None),
                        'account_number': getattr(parsed_statement, 'account_number', None),
                        'pdf_name': filename,
                    }
                    
                    db = DatabaseConnection(**DB_CONFIG)
                    db.connect()
                    db_statement_id = import_statement_to_db(db, statement_data, bank_name)
                    db.disconnect()
                    
                    # Store for analytics
                    parsed_statement.enriched_df = enriched_df
                    parsed_statement.monthly_income_df = monthly_income_df
                    parsed_statement.income_summary = income_summary
                    parsed_statements.append(parsed_statement)
                    
                    all_results.append({
                        "statement_id": statement_id,
                        "statement_name": filename,
                        "status": "success",
                        "message": f"Успешно обработано и сохранено в БД (ID: {db_statement_id})",
                        "bank": getattr(parsed_statement, "bank", "Неизвестно"),
                        "iin": iin,
                        "income_summary": income_summary,
                        "db_statement_id": db_statement_id,
                        "parsed_statement": parsed_statement
                    })
                    
                except Exception as e:
                    all_results.append({
                        "statement_id": statement_id,
                        "statement_name": filename,
                        "status": "error",
                        "message": f"Ошибка сохранения в БД: {str(e)}",
                        "bank": getattr(parsed_statement, "bank", "Неизвестно"),
                        "iin": iin,
                        "error": str(e),
                        "parsed_statement": None
                    })
            else:
                # Failed parsing
                all_results.append({
                    "statement_id": statement_id,
                    "statement_name": filename,
                    "status": "error" if status == processor.STATUS_FAILURE else "warning",
                    "message": parse_result.get("message", ""),
                    "bank": getattr(parsed_statement, "bank", "Неизвестно") if parsed_statement else "Неизвестно",
                    "iin": iin if parsed_statement else None,
                    "error": parse_result.get("error"),
                    "parsed_statement": None
                })
        
        # Calculate analytics for this IIN group
        analytics = {}
        if parsed_statements:
            analytics = processor.calculate_analytics(parsed_statements)
        
        # Determine project status
        if has_data_mismatch:
            project_status = 2
            response_message = "Расхождение регистрационных данных"
        elif has_failure:
            project_status = 1
            response_message = "Провал"
        else:
            project_status = 0
            response_message = "Успех"
        
        # Create project (like upload_initial)
        project = storage.create_project(
            iin=iin,
            statements=statements_resp,
            analytics=analytics,
            status=project_status
        )
        
        # Save statement files
        for statement_id, pdf_bytes, filename in statement_files_data:
            storage.save_statement_file(
                project_id=project.project_id,
                statement_id=statement_id,
                file_data=pdf_bytes,
                filename=filename
            )
        
        projects_created.append({
            "project_id": project.project_id,
            "iin": iin,
            "status": project_status,
            "message": response_message,
            "create_date": project.create_date,
            "analytics": analytics,
            "statements_count": len(statements_resp)
        })
    
    # Process statements without IIN (create separate project or mark as error)
    statements_without_iin = []
    for statement_id, pdf_bytes, filename, parse_result in statements_data:
        parsed_statement = parse_result.get("parsed_statement")
        if not parsed_statement:
            iin = None
        else:
            iin = getattr(parsed_statement, "iin_bin", None)
            if iin:
                iin = iin.strip()
        
        # If statement doesn't have IIN or wasn't processed above
        if not iin or iin not in parsed_statements_by_iin:
            statements_without_iin.append({
                "statement_id": statement_id,
                "pdf_bytes": pdf_bytes,
                "filename": filename,
                "parse_result": parse_result,
                "parsed_statement": parsed_statement
            })
    
    # Create project for statements without IIN (use "UNKNOWN" as IIN)
    if statements_without_iin:
        statements_resp = []
        parsed_statements = []
        statement_files_data = []
        has_failure = False
        
        for stmt_data in statements_without_iin:
            parse_result = stmt_data["parse_result"]
            parsed_statement = stmt_data["parsed_statement"]
            statement_id = stmt_data["statement_id"]
            pdf_bytes = stmt_data["pdf_bytes"]
            filename = stmt_data["filename"]
            
            status = parse_result.get("status")
            if status == processor.STATUS_FAILURE or status == processor.STATUS_SCANNED_COPY:
                has_failure = True
            
            ext = ".pdf" if filename.lower().endswith('.pdf') else ""
            if ext and not filename.endswith(ext):
                filename = f"{filename}{ext}"
            statement_files_data.append((statement_id, pdf_bytes, filename))
            
            statements_resp.append({
                'id': statement_id,
                'name': filename,
                'extension': ext,
                'status': status,
                'message': parse_result.get("message", "ИИН не найден в выписке")
            })
            
            if parsed_statement and status == processor.STATUS_SUCCESS:
                parsed_statements.append(parsed_statement)
            
            all_results.append({
                "statement_id": statement_id,
                "statement_name": filename,
                "status": "error" if status == processor.STATUS_FAILURE else "warning",
                "message": parse_result.get("message", "ИИН не найден в выписке"),
                "bank": getattr(parsed_statement, "bank", "Неизвестно") if parsed_statement else "Неизвестно",
                "iin": "Не найден",
                "error": parse_result.get("error") or "ИИН не найден в выписке. Проверка через API Солик недоступна.",
                "parsed_statement": None
            })
        
        # Create project with "UNKNOWN" IIN
        if statements_resp:
            project_status = 1 if has_failure else 0
            response_message = "ИИН не найден в выписках" if has_failure else "Успех (ИИН не найден)"
            
            project = storage.create_project(
                iin="UNKNOWN",
                statements=statements_resp,
                analytics={},
                status=project_status
            )
            
            # Save files
            for statement_id, pdf_bytes, filename in statement_files_data:
                storage.save_statement_file(
                    project_id=project.project_id,
                    statement_id=statement_id,
                    file_data=pdf_bytes,
                    filename=filename
                )
            
            projects_created.append({
                "project_id": project.project_id,
                "iin": "UNKNOWN",
                "status": project_status,
                "message": response_message,
                "create_date": project.create_date,
                "analytics": {},
                "statements_count": len(statements_resp)
            })
    
    return {
        "results": all_results,
        "projects": projects_created
    }


def process_statements_for_project(
    uploaded_files: List,
    project_id: str,
    processor: StatementProcessor,
    anchor_date: Optional[date] = None
) -> Dict[str, Any]:
    """
    Process uploaded statements and attach each result to a selected DB project.
    Limits must be validated before call.
    """
    results: List[Dict[str, Any]] = []
    existing_count = _count_project_statements(project_id)
    processed = skipped = failed = 0

    _update_project_status(project_id, "processing")

    for idx, uploaded_file in enumerate(uploaded_files, start=1):
        statement_id = str(uuid.uuid4())
        filename = uploaded_file.name
        pdf_bytes = uploaded_file.read()
        base64_data = base64.b64encode(pdf_bytes).decode("utf-8")

        parse_result = processor.parse_statement_base64(
            statement_id=statement_id,
            statement_name=filename,
            extension=".pdf",
            base64_data=base64_data,
            expected_iin=None
        )

        parsed_statement = parse_result.get("parsed_statement")
        status_code = parse_result.get("status")
        upload_order = existing_count + idx

        if not parsed_statement or status_code != processor.STATUS_SUCCESS:
            message = parse_result.get("message", "Ошибка парсинга")
            _link_statement_to_project(
                project_id=project_id,
                statement_id=None,
                upload_order=upload_order,
                source_filename=filename,
                processing_status="error",
                processing_message=message,
            )
            failed += 1
            results.append({
                "statement_id": statement_id,
                "statement_name": filename,
                "status": "error",
                "message": message,
                "bank": getattr(parsed_statement, "bank", "Неизвестно") if parsed_statement else "Неизвестно",
                "iin": getattr(parsed_statement, "iin_bin", None) if parsed_statement else None,
                "parsed_statement": parsed_statement,
            })
            continue

        iin = (getattr(parsed_statement, "iin_bin", None) or "").strip()
        if not iin:
            msg = "Пропущено: нет ИИН/БИН/ИНН данных для IP расчета"
            _link_statement_to_project(
                project_id=project_id,
                statement_id=None,
                upload_order=upload_order,
                source_filename=filename,
                processing_status="skipped",
                processing_message=msg,
            )
            skipped += 1
            results.append({
                "statement_id": statement_id,
                "statement_name": filename,
                "status": "warning",
                "message": msg,
                "bank": getattr(parsed_statement, "bank", "Неизвестно"),
                "iin": None,
                "parsed_statement": parsed_statement,
            })
            continue

        try:
            calc_date = _resolve_income_anchor_date(parsed_statement, fallback=anchor_date)
            window_start, window_end = get_last_full_12m_window(calc_date)
            enriched_df, income_summary = compute_ip_income_for_statement(
                parsed_statement,
                window_start,
                window_end
            )
            monthly_income_df = _build_monthly_ip_income_df(enriched_df)

            statement_data = {
                'header_df': getattr(parsed_statement, 'header_df', None),
                'tx_df': getattr(parsed_statement, 'tx_df', None),
                'footer_df': getattr(parsed_statement, 'footer_df', None),
                'meta_df': getattr(parsed_statement, 'meta_df', None),
                'tx_ip_df': enriched_df,
                'monthly_income_df': monthly_income_df,
                'income_summary': income_summary if income_summary else {},
                'client_iin': iin,
                'client_name': getattr(parsed_statement, 'account_holder_name', None),
                'account_number': getattr(parsed_statement, 'account_number', None),
                'pdf_name': filename,
            }

            bank_name = format_bank_name(getattr(parsed_statement, "bank", "Неизвестно"))
            db = DatabaseConnection(**DB_CONFIG)
            db.connect()
            db_statement_id = import_statement_to_db(db, statement_data, bank_name)
            db.disconnect()

            _link_statement_to_project(
                project_id=project_id,
                statement_id=str(db_statement_id),
                upload_order=upload_order,
                source_filename=filename,
                processing_status="success",
                processing_message="Успешно обработано",
            )

            parsed_statement.enriched_df = enriched_df
            parsed_statement.monthly_income_df = monthly_income_df
            parsed_statement.income_summary = income_summary
            processed += 1
            results.append({
                "statement_id": statement_id,
                "statement_name": filename,
                "status": "success",
                "message": f"Успешно обработано и привязано к проекту {project_id}",
                "bank": getattr(parsed_statement, "bank", "Неизвестно"),
                "iin": iin,
                "income_summary": income_summary,
                "db_statement_id": db_statement_id,
                "parsed_statement": parsed_statement,
            })
        except Exception as e:
            _link_statement_to_project(
                project_id=project_id,
                statement_id=None,
                upload_order=upload_order,
                source_filename=filename,
                processing_status="error",
                processing_message=f"Ошибка БД: {e}",
            )
            failed += 1
            results.append({
                "statement_id": statement_id,
                "statement_name": filename,
                "status": "error",
                "message": f"Ошибка сохранения в БД: {e}",
                "bank": getattr(parsed_statement, "bank", "Неизвестно"),
                "iin": iin,
                "parsed_statement": parsed_statement,
            })

    if failed > 0 and processed == 0:
        _update_project_status(project_id, "failed")
    elif failed > 0 or skipped > 0:
        _update_project_status(project_id, "completed_with_warnings")
    else:
        _update_project_status(project_id, "completed")

    return {
        "results": results,
        "processed": processed,
        "skipped": skipped,
        "failed": failed,
    }


def process_and_save_statement(
    statement_id: str,
    statement_name: str,
    pdf_bytes: bytes,
    processor: StatementProcessor,
    anchor_date: Optional[date] = None
) -> Dict[str, Any]:
    """
    Process statement and save to database.
    Returns result dict with status, message, and data.
    """
    result = {
        "statement_id": statement_id,
        "statement_name": statement_name,
        "status": "pending",
        "message": "",
        "bank": None,
        "iin": None,
        "income_summary": None,
        "error": None,
        "db_statement_id": None,
    }
    
    try:
        # Encode to base64 for processor
        base64_data = base64.b64encode(pdf_bytes).decode('utf-8')
        extension = ".pdf" if statement_name.lower().endswith('.pdf') else ""
        
        # Parse statement (automatic bank detection)
        parse_result = processor.parse_statement_base64(
            statement_id=statement_id,
            statement_name=statement_name,
            extension=extension,
            base64_data=base64_data,
            expected_iin=None  # No IIN validation for now
        )
        
        result["status_code"] = parse_result.get("status")
        result["message"] = parse_result.get("message", "")
        result["error"] = parse_result.get("error")
        
        parsed_statement = parse_result.get("parsed_statement")
        
        if not parsed_statement:
            result["status"] = "error"
            result["message"] = f"Ошибка парсинга: {result.get('error', 'Неизвестная ошибка')}"
            return result
        
        # Extract bank and IIN
        result["bank"] = getattr(parsed_statement, "bank", "Неизвестно")
        result["iin"] = getattr(parsed_statement, "iin_bin", None)
        
        # Check if parsing was successful
        if parse_result.get("status") != processor.STATUS_SUCCESS:
            result["status"] = "warning"
            if parse_result.get("status") == processor.STATUS_SCANNED_COPY:
                result["message"] = "Загружены сканированные копии документа"
            elif parse_result.get("status") == processor.STATUS_DATA_MISMATCH:
                result["message"] = "Расхождение регистрационных данных"
            else:
                result["status"] = "error"
            return result
        
        # Calculate income in the same way as batch parsers:
        # anchor by statement date, fallback to UI-selected date.
        calc_date = _resolve_income_anchor_date(parsed_statement, fallback=anchor_date)
        window_start, window_end = get_last_full_12m_window(calc_date)
        
        enriched_df = None
        income_summary = None
        monthly_income_df = None
        
        try:
            enriched_df, income_summary = compute_ip_income_for_statement(
                parsed_statement,
                window_start,
                window_end
            )
            result["income_summary"] = income_summary
            
            # Extract monthly income from enriched_df
            monthly_income_df = _build_monthly_ip_income_df(enriched_df)
        except Exception as e:
            result["error"] = f"Ошибка расчета дохода: {str(e)}"
            result["status"] = "warning"
        
        # Prepare data for database
        try:
            # Get bank name for database
            bank_name = format_bank_name(result["bank"])
            
            # Prepare statement data
            statement_data = {
                'header_df': getattr(parsed_statement, 'header_df', None),
                'tx_df': getattr(parsed_statement, 'tx_df', None),
                'footer_df': getattr(parsed_statement, 'footer_df', None),
                'meta_df': getattr(parsed_statement, 'meta_df', None),
                'tx_ip_df': enriched_df,
                'monthly_income_df': monthly_income_df,
                'income_summary': income_summary if income_summary else {},
                'client_iin': result["iin"],
                'client_name': getattr(parsed_statement, 'account_holder_name', None),
                'account_number': getattr(parsed_statement, 'account_number', None),
                'pdf_name': statement_name,
            }
            
            # Save to database
            db = DatabaseConnection(**DB_CONFIG)
            db.connect()
            
            db_statement_id = import_statement_to_db(db, statement_data, bank_name)
            db.disconnect()
            
            result["db_statement_id"] = db_statement_id
            result["status"] = "success"
            result["message"] = f"Успешно обработано и сохранено в БД (ID: {db_statement_id})"
            
            # Store processed statement for later use
            parsed_statement.enriched_df = enriched_df if 'enriched_df' in locals() else None
            parsed_statement.monthly_income_df = monthly_income_df
            parsed_statement.income_summary = income_summary
            result["parsed_statement"] = parsed_statement
            
        except Exception as e:
            result["error"] = f"Ошибка сохранения в БД: {str(e)}"
            result["status"] = "error"
            result["message"] = f"Ошибка сохранения в БД: {str(e)}"
    
    except Exception as e:
        result["status"] = "error"
        result["error"] = str(e)
        result["message"] = f"Критическая ошибка: {str(e)}"
    
    return result


def display_results(results: List[Dict[str, Any]]):
    """Display processing results"""
    if not results:
        return
    
    st.header("📊 Результаты обработки")
    
    # Summary statistics
    success_count = sum(1 for r in results if r.get("status") == "success")
    error_count = sum(1 for r in results if r.get("status") == "error")
    warning_count = sum(1 for r in results if r.get("status") == "warning")
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Всего", len(results))
    with col2:
        st.metric("Успешно", success_count, delta=f"+{success_count}")
    with col3:
        st.metric("Ошибки", error_count, delta=f"-{error_count}" if error_count > 0 else None)
    with col4:
        st.metric("Предупреждения", warning_count)
    
    # Detailed results table
    st.subheader("Детали обработки")
    
    results_data = []
    for r in results:
        income = r.get("income_summary", {})
        total_income = income.get("total_income_adjusted", 0) if income else 0
        
        status_ru = {
            "success": "Успешно",
            "error": "Ошибка",
            "warning": "Предупреждение",
            "pending": "В обработке"
        }.get(r.get("status", "unknown"), r.get("status", "неизвестно"))
        
        results_data.append({
            "Файл": r.get("statement_name", "Неизвестно"),
            "Банк": format_bank_name(r.get("bank", "Неизвестно")),
            "ИИН": r.get("iin", "Не найден"),
            "Статус": status_ru,
            "Доход (12 мес)": f"{total_income:,.2f} ₸" if total_income > 0 else "Не рассчитан",
            "Сообщение": r.get("message", ""),
            "ID в БД": r.get("db_statement_id", "Не сохранено"),
        })
    
    if results_data:
        df_results = pd.DataFrame(results_data)
        st.dataframe(df_results, use_container_width=True, hide_index=True)
    
    # Errors and warnings
    errors = [r for r in results if r.get("status") == "error"]
    warnings = [r for r in results if r.get("status") == "warning"]
    
    if errors:
        st.error("❌ Ошибки обработки:")
        for err in errors:
            st.error(f"**{err.get('statement_name')}**: {err.get('message')}")
            if err.get("error"):
                with st.expander("Детали ошибки"):
                    st.code(err.get("error"))
    
    if warnings:
        st.warning("⚠️ Предупреждения:")
        for warn in warnings:
            st.warning(f"**{warn.get('statement_name')}**: {warn.get('message')}")
    
    # Income summaries
    successful = [r for r in results if r.get("status") == "success" and r.get("income_summary")]
    if successful:
        st.subheader("💰 Расчет дохода")
        
        total_income_all = 0
        for r in successful:
            income = r.get("income_summary", {})
            total_income = income.get("total_income_adjusted", 0) if income else 0
            total_income_all += total_income
            
            st.info(f"**{r.get('statement_name')}** ({format_bank_name(r.get('bank'))}): "
                   f"Доход за 12 месяцев: **{total_income:,.2f} ₸**")
        
        if len(successful) > 1:
            st.success(f"**Общий доход по всем выпискам: {total_income_all:,.2f} ₸**")


def display_admin_tables(processed_statements: List[Any]):
    """Display admin tables from processed statements"""
    if not processed_statements:
        return
    
    st.header("📋 Админка - Аналитические таблицы")
    
    # Чекбокс для выбора - учитывать даты или нет
    if "filter_by_date" not in st.session_state:
        st.session_state.filter_by_date = False
    
    filter_by_date = st.checkbox(
        "Фильтровать транзакции по датам",
        value=st.session_state.filter_by_date,
        help="Если включено, учитываются только транзакции в указанном диапазоне дат. Если выключено, учитываются все транзакции."
    )
    st.session_state.filter_by_date = filter_by_date
    
    # Combine transactions from all statements
    anchor_date = st.session_state.anchor_date
    _, window_end_calc = get_last_full_12m_window(anchor_date)
    
    all_statements = [r.get("parsed_statement") for r in processed_statements 
                     if r.get("parsed_statement") and hasattr(r.get("parsed_statement"), "tx_df")]
    
    if not all_statements:
        st.info("Нет данных для отображения")
        return
    
    total_tx_before = 0
    for stmt in all_statements:
        if stmt and hasattr(stmt, "tx_df") and not stmt.tx_df.empty:
            total_tx_before += len(stmt.tx_df)
    
    # Отладочная информация
    if total_tx_before == 0:
        st.warning(f"⚠️ Всего транзакций в выписках: {total_tx_before}")
        st.info("Нет транзакций для обработки")
        return
    
    # Определяем окно дат (используется только если filter_by_date = True)
    # Для теста: год начиная с anchor_date
    window_start, window_end = get_last_full_12m_window(anchor_date)
    
    if filter_by_date:
        st.info(f"📅 Фильтрация по датам включена. Окно: {window_start} → {window_end}")
    else:
        st.info("📅 Фильтрация по датам выключена. Учитываются все транзакции.")
    
    # Объединяем транзакции (с фильтрацией или без)
    # Используем **kwargs для передачи filter_by_date, чтобы избежать ошибок, если параметр не поддерживается
    try:
        tx_12m = combine_transactions(all_statements, window_start, window_end, filter_by_date=filter_by_date)
    except TypeError as e:
        # Если функция не поддерживает filter_by_date, вызываем без него (для обратной совместимости)
        if "filter_by_date" in str(e):
            tx_12m = combine_transactions(all_statements, window_start, window_end)
            # Вручную фильтруем, если нужно
            if filter_by_date:
                if not tx_12m.empty and "txn_date" in tx_12m.columns:
                    mask = (tx_12m["txn_date"] >= pd.Timestamp(window_start)) & (tx_12m["txn_date"] <= pd.Timestamp(window_end))
                    tx_12m = tx_12m.loc[mask].copy()
        else:
            raise
    
    if tx_12m.empty:
        st.warning(f"⚠️ Всего транзакций в выписках: {total_tx_before}")
        if filter_by_date:
            st.warning(f"⚠️ Окно анализа: {window_start} → {window_end}")
        # Показываем примеры дат из выписок для отладки
        for stmt in all_statements:
            if stmt and hasattr(stmt, "tx_df") and not stmt.tx_df.empty:
                if "txn_date" in stmt.tx_df.columns:
                    sample_dates = stmt.tx_df["txn_date"].head(3).tolist()
                    unique_dates = stmt.tx_df["txn_date"].unique()[:5]
                    st.write(f"**{stmt.pdf_name}**: Примеры дат: {sample_dates}, Уникальные: {[str(d) for d in unique_dates]}")
        st.info("Нет транзакций для анализа")
        return
    
    # Prepare data for analysis
    df_analysis = tx_12m.copy()
    
    # Clean amounts
    def clean_amt_val(v):
        if pd.isna(v) or v == '':
            return 0.0
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).replace(',', '').replace(' ', '').replace('\xa0', '').replace('\u00A0', '').strip()
        try:
            return float(s)
        except:
            return 0.0
    
    # Determine amount column
    if 'Дебет' in df_analysis.columns and 'Кредит' in df_analysis.columns:
        d_clean = df_analysis['Дебет'].apply(clean_amt_val)
        k_clean = df_analysis['Кредит'].apply(clean_amt_val)
        df_analysis['amount'] = k_clean - d_clean
    elif 'amount' not in df_analysis.columns:
        amt_col = next((c for c in ['Сумма операции', 'Сумма', 'Расход', 'Кредит'] 
                       if c in df_analysis.columns), None)
        if amt_col:
            df_analysis['amount'] = df_analysis[amt_col].apply(clean_amt_val)
        else:
            df_analysis['amount'] = 0.0
    
    # Determine description
    desc_col = next((c for c in ['Детали платежа', 'Описание операции', 'details', 
                                'Назначение платежа', 'operation'] if c in df_analysis.columns), None)
    df_analysis['details'] = df_analysis[desc_col].fillna('') if desc_col else ''
    
    # Determine counterparty
    import re
    def get_cp_data(row):
        cp_candidates = ['Контрагент', 'Контрагент (имя)', 'Корреспондент', 'Наименование получателя']
        cp_text = ""
        for col in cp_candidates:
            if col in row and pd.notna(row[col]):
                cp_text = str(row[col])
                break
        
        bin_match = re.search(r'(\d{12})', cp_text)
        if bin_match:
            bin_val = bin_match.group(1)
            name = cp_text.split('БИН')[0].split('ИИН')[0].split('\n')[0].strip()
            return bin_val, (name if name else bin_val)
        
        name_fallback = cp_text.split('\n')[0].strip() if cp_text else (row.get('details') or 'N/A')
        return str(name_fallback), str(name_fallback)
    
    cp_results = df_analysis.apply(get_cp_data, axis=1)
    df_analysis['counterparty_id'] = [x[0] for x in cp_results]
    df_analysis['counterparty_name'] = [x[1] for x in cp_results]
    
    # Generate analysis tables
    analysis = get_ui_analysis_tables(df_analysis)
    
    # Display tables
    col1, col2 = st.columns(2)
    with col1:
        st.write("**Расходы (Дебет)**")
        if analysis["debit_top"]:
            st.dataframe(pd.DataFrame(analysis["debit_top"]), use_container_width=True, hide_index=True)
        else:
            st.info("Нет данных по расходам")
    
    with col2:
        st.write("**Приходы (Кредит)**")
        if analysis["credit_top"]:
            st.dataframe(pd.DataFrame(analysis["credit_top"]), use_container_width=True, hide_index=True)
        else:
            st.info("Нет данных по приходам")
    
    st.subheader("Аффилированные лица (Net расчет)")
    if analysis["related_parties"]:
        rp_df = pd.DataFrame(analysis["related_parties"])
        st.dataframe(rp_df.sort_values("Оборот", ascending=False), use_container_width=True, hide_index=True)
    else:
        st.info("Нет данных по аффилированным лицам")


def display_statement_source_tables(processed_results: List[Dict[str, Any]]) -> None:
    """
    Display only tx_ip table for each processed statement.
    """
    if not processed_results:
        return

    rows_with_statement = [r for r in processed_results if r.get("parsed_statement")]
    if not rows_with_statement:
        return

    st.header("📑 Таблицы выписки (CSV-эквивалент)")

    for idx, row in enumerate(rows_with_statement):
        stmnt = row.get("parsed_statement")
        if stmnt is None:
            continue

        pdf_name = getattr(stmnt, "pdf_name", row.get("statement_name", f"statement_{idx + 1}"))
        bank_name = format_bank_name(getattr(stmnt, "bank", "Неизвестно"))
        tx_ip_df = getattr(stmnt, "enriched_df", None)

        with st.expander(f"{pdf_name} ({bank_name})", expanded=(idx == 0)):
            if tx_ip_df is not None and not tx_ip_df.empty:
                st.dataframe(tx_ip_df, use_container_width=True, hide_index=True)
            else:
                st.info("tx_ip: нет данных")


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


def display_taxpayer_search_tab():
    """Отображение вкладки поиска налогоплательщика"""
    st.header("🔍 Поиск Налогоплательщика")
    st.markdown("""
    **Поиск информации о налогоплательщике через API сервиса «Поиск Налогоплательщика».**
    
    Поддерживаются следующие типы налогоплательщиков:
    - **ИП** (Индивидуальный предприниматель)
    - **ЛЗЧП** (Лицо, занимающееся частной практикой)
    - **ЮЛ** (Юридическое лицо)
    """)
    
    # Форма поиска
    with st.form("taxpayer_search_form"):
        portal_host = st.text_input(
            "Portal Host *",
            value=TAXPAYER_API_PORTAL_HOST,
            help="Базовый URL портала сервиса поиска"
        )
        portal_token = st.text_input(
            "X-Portal-Token *",
            value="",
            type="password",
            help="Укажите токен вручную для текущей сессии"
        )
        st.divider()
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
            if taxpayer_type == "LZCHP":
                first_name = st.text_input("Имя *", placeholder="First")
                last_name = st.text_input("Фамилия *", placeholder="Last")
                name = None
            else:
                name = st.text_input("Наименование *", placeholder="TOO")
                first_name = None
                last_name = None
        
        submitted = st.form_submit_button("🔍 Найти", type="primary")
    
    if submitted:
        if not portal_host.strip():
            st.error("❌ Укажите Portal Host")
            return
        if not portal_token.strip():
            st.error("❌ Укажите X-Portal-Token")
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
        
        with st.spinner("🔍 Выполняется поиск..."):
            try:
                client = TaxpayerAPIClient(
                    portal_host=portal_host.strip(),
                    portal_token=portal_token.strip()
                )
                
                taxpayer_type_enum = TaxpayerType[taxpayer_type]
                result = client.search_taxpayer(
                    taxpayer_code=taxpayer_code,
                    taxpayer_type=taxpayer_type_enum,
                    name=name,
                    first_name=first_name,
                    last_name=last_name,
                    print=False
                )
                
                search_record = {
                    "taxpayer_code": taxpayer_code,
                    "taxpayer_type": taxpayer_type,
                    "result": result,
                }
                st.session_state.taxpayer_search_results.insert(0, search_record)
                
                st.success("✅ Поиск выполнен!")
                
                if result.get("success"):
                    st.subheader("📊 Результат поиска")
                    data = result.get("data", {})
                    with st.expander("📋 JSON ответ", expanded=True):
                        st.json(data)
                    formatted = format_taxpayer_response(data)
                    if formatted:
                        st.markdown("### 📝 Форматированный результат")
                        st.markdown(formatted)
                else:
                    st.error(f"❌ Ошибка поиска: {result.get('error', 'Неизвестная ошибка')}")
                    if result.get("message"):
                        st.error(f"Детали: {result['message']}")
            
            except Exception as e:
                st.error(f"❌ Критическая ошибка: {str(e)}")
                st.exception(e)
    
    # История поисков
    if st.session_state.taxpayer_search_results:
        st.divider()
        st.header("📜 История поисков")
        for idx, record in enumerate(st.session_state.taxpayer_search_results[:5]):
            with st.expander(f"🔍 {record['taxpayer_type']} - {record['taxpayer_code']}"):
                result = record["result"]
                if result.get("success"):
                    data = result.get("data", {})
                    st.json(data)
                else:
                    st.error(f"Ошибка: {result.get('error', 'Неизвестная ошибка')}")


def main() -> None:
    """Main application"""
    st.set_page_config(
        page_title="Загрузка банковских выписок",
        page_icon="📄",
        layout="wide"
    )
    
    init_session_state()
    
    # Вкладки для переключения между функциями
    tab1, tab2 = st.tabs(["📄 Загрузка выписок", "🔍 Поиск налогоплательщика"])
    
    with tab1:
        st.title("📄 Загрузка банковских выписок")
        st.markdown("""
        **Загрузите одну или несколько выписок. Система автоматически:**
        - 🔍 Определит банк для каждой выписки
        - 🆔 Извлечет ИИН из выписки
        - 📁 Сгруппирует выписки по ИИН
        - 💾 Сохранит все данные в БД
        - 💰 Рассчитает доход за 12 месяцев
        """)
        
        # Ensure schema for project workflow
        try:
            _ensure_project_schema()
        except Exception as e:
            st.error(f"Не удалось инициализировать схему проектов: {e}")
            return

        # Date selection for testing
        with st.sidebar:
            st.header("⚙️ Настройки")
            api_ok, api_msg = check_api_health()
            if api_ok:
                st.success(f"🔌 API: {api_msg}")
            else:
                st.warning(f"🔌 API: {api_msg}")
            st.caption(f"API_BASE_URL: {API_BASE_URL}")
            st.session_state.anchor_date = st.date_input(
                "📅 Дата для расчета (тестирование)",
                value=st.session_state.anchor_date,
                help="Используется для расчета 12-месячного окна дохода"
            )
            
            # Show calculated window (год начиная с выбранной даты)
            window_start, window_end = get_last_full_12m_window(st.session_state.anchor_date)
            st.info(f"**Окно анализа (последние 12 полных месяцев):**\n{window_start} → {window_end}")
            
            if st.button("🔄 Сбросить на сегодня"):
                st.session_state.anchor_date = date.today()
                st.rerun()

        st.header("0. Проект")
        project_col1, project_col2 = st.columns([2, 1])
        with project_col1:
            projects = _list_projects()
            project_options = [None] + [str(p["id"]) for p in projects]
            project_label_map = {None: "Выберите проект"}
            for p in projects:
                project_label_map[str(p["id"])] = f'{p["name"]} ({p["statements_count"]}/9, {p["status"]})'

            st.session_state.selected_project_id = st.selectbox(
                "Текущий проект",
                options=project_options,
                index=project_options.index(st.session_state.selected_project_id)
                if st.session_state.selected_project_id in project_options else 0,
                format_func=lambda x: project_label_map.get(x, str(x)),
                help="Сначала создайте или выберите проект",
            )
        with project_col2:
            new_project_name = st.text_input("Название проекта", value="")
            if st.button("➕ Создать проект", type="secondary"):
                if not new_project_name.strip():
                    st.warning("Введите название проекта")
                else:
                    pid = _create_project(new_project_name.strip())
                    st.session_state.selected_project_id = pid
                    st.success(f"Проект создан: {pid}")
                    st.rerun()
    
        # File upload section
        st.header("1. Загрузка выписок")
        uploaded_files = st.file_uploader(
            "Выберите файлы выписок (PDF)",
            type=["pdf"],
            accept_multiple_files=True,
            help="Можно загрузить несколько выписок разных банков одновременно"
        )
        
        processor = StatementProcessor()
        
        selected_project_id = st.session_state.selected_project_id
        if uploaded_files and st.button(
            "🚀 Обработать и сохранить в проект",
            type="primary",
            disabled=not bool(selected_project_id)
        ):
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            status_text.text(f"Обработка {len(uploaded_files)} выписок...")
            progress_bar.progress(0.1)
            
            # Process statements and attach to selected project
            try:
                if not selected_project_id:
                    raise ValueError("Сначала выберите проект")

                existing_count = _count_project_statements(selected_project_id)
                if existing_count + len(uploaded_files) > 9:
                    raise ValueError(f"В проекте может быть максимум 9 выписок. Уже загружено: {existing_count}")

                result_data = process_statements_for_project(
                    uploaded_files,
                    project_id=selected_project_id,
                    processor=processor,
                    anchor_date=st.session_state.anchor_date
                )
                
                progress_bar.progress(1.0)
                progress_bar.empty()
                status_text.empty()
                
                # Store results
                st.session_state.upload_results = result_data["results"]
                st.session_state.processed_statements = [
                    r for r in result_data["results"]
                    if r.get("parsed_statement") and r.get("status") == "success"
                ]
                st.session_state.projects_created = []

                st.success(
                    f"✅ Проект {selected_project_id}: "
                    f"успешно {result_data['processed']}, "
                    f"пропущено {result_data['skipped']}, "
                    f"ошибок {result_data['failed']}"
                )
                st.rerun()
                
            except Exception as e:
                progress_bar.empty()
                status_text.empty()
                st.error(f"❌ Критическая ошибка при обработке: {str(e)}")
                st.exception(e)
        
        # Display projects created
        if st.session_state.projects_created:
            st.header("📁 Созданные проекты")
        projects_data = []
        for p in st.session_state.projects_created:
            projects_data.append({
                "ID проекта": p["project_id"],
                "ИИН": p["iin"],
                "Статус": "Успех" if p["status"] == 0 else ("Провал" if p["status"] == 1 else "Расхождение данных"),
                "Сообщение": p["message"],
                "Количество выписок": p["statements_count"],
                "Дата создания": p["create_date"].strftime("%d.%m.%Y %H:%M:%S") if isinstance(p["create_date"], datetime) else str(p["create_date"]),
            })
        
        if projects_data:
            df_projects = pd.DataFrame(projects_data)
            st.dataframe(df_projects, use_container_width=True, hide_index=True)
            
            # Show analytics for each project
            for p in st.session_state.projects_created:
                if p.get("analytics"):
                    with st.expander(f"📊 Аналитика проекта {p['project_id']} (ИИН: {p['iin']})"):
                        analytics = p["analytics"]
                        if analytics.get("iin"):
                            st.write(f"**ИИН:** {analytics['iin']}")
                        if analytics.get("registration_date"):
                            st.write(f"**Дата регистрации:** {analytics['registration_date']}")
                        if analytics.get("average_income"):
                            st.write(f"**Средний доход:** {analytics['average_income']:,.2f} ₸")
    
        # Display results if available
        if st.session_state.upload_results:
            display_results(st.session_state.upload_results)
            display_statement_source_tables(st.session_state.upload_results)
            
            # Display admin tables
            if st.session_state.processed_statements:
                st.divider()
                display_admin_tables(st.session_state.processed_statements)
        
        # Clear results button
        if st.session_state.upload_results:
            if st.button("🗑️ Очистить результаты"):
                st.session_state.upload_results = []
                st.session_state.processed_statements = []
                st.session_state.projects_created = []
                st.rerun()
        
        # Database management section
        st.divider()
        with st.expander("🗄️ Управление базой данных", expanded=False):
            st.warning("⚠️ Опасная зона: операции с базой данных")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Очистка базы данных")
                st.write("Удаляет все данные из всех таблиц. Структура БД сохраняется.")
                
                confirm_text = st.text_input(
                    "Введите 'ОЧИСТИТЬ' для подтверждения:",
                    key="clear_db_confirm",
                    help="Это действие необратимо!"
                )
                
                if st.button("🗑️ Очистить БД", type="secondary", disabled=confirm_text != "ОЧИСТИТЬ"):
                    try:
                        db = DatabaseConnection(**DB_CONFIG)
                        db.connect()
                        cursor = db.connection.cursor()
                        
                        # Отключить проверку внешних ключей
                        cursor.execute("SET session_replication_role = 'replica';")
                        
                        # Очистить таблицы
                        tables = [
                            'transactions_ip_flags',
                            'transactions',
                            'ip_income_monthly',
                            'income_summaries',
                            'statement_metadata',
                            'statement_footers',
                            'statement_headers',
                            'counterparties',
                            'statements',
                            'accounts',
                            'clients'
                        ]
                        
                        cleared = []
                        for table in tables:
                            try:
                                db.safe_truncate_table(table)
                                cleared.append(table)
                            except Exception as e:
                                st.error(f"Ошибка при очистке {table}: {e}")
                        
                        # Включить обратно проверку внешних ключей
                        cursor.execute("SET session_replication_role = 'origin';")
                        db.connection.commit()
                        
                        # Проверка
                        counts = {}
                        table_names_ru = {
                            'clients': 'Клиенты',
                            'accounts': 'Счета',
                            'statements': 'Выписки',
                            'transactions': 'Транзакции'
                        }
                        for table in ['clients', 'accounts', 'statements', 'transactions']:
                            counts[table_names_ru[table]] = db.safe_count_table(table)
                        
                        cursor.close()
                        db.disconnect()
                        
                        st.success(f"✅ База данных очищена! Очищено таблиц: {len(cleared)}")
                        st.json(counts)
                        st.rerun()
                        
                    except Exception as e:
                        st.error(f"❌ Ошибка при очистке БД: {e}")
                        st.exception(e)
            
            with col2:
                st.subheader("Статистика базы данных")
                
                if st.button("📊 Обновить статистику"):
                    try:
                        db = DatabaseConnection(**DB_CONFIG)
                        db.connect()
                        
                        stats = {}
                        tables = ['clients', 'accounts', 'statements', 'transactions', 'income_summaries']
                        table_names_ru = {
                            'clients': 'Клиенты',
                            'accounts': 'Счета',
                            'statements': 'Выписки',
                            'transactions': 'Транзакции',
                            'income_summaries': 'Расчеты дохода'
                        }
                        
                        for table in tables:
                            try:
                                stats[table_names_ru[table]] = db.safe_count_table(table)
                            except Exception:
                                stats[table_names_ru[table]] = "Н/Д"
                        
                        db.disconnect()
                        
                        st.json(stats)
                        
                        # Отображение в виде метрик
                        col_a, col_b, col_c = st.columns(3)
                        with col_a:
                            st.metric("Клиенты", stats.get('Клиенты', 0))
                        with col_b:
                            st.metric("Счета", stats.get('Счета', 0))
                        with col_c:
                            st.metric("Выписки", stats.get('Выписки', 0))
                    
                    except Exception as e:
                        st.error(f"❌ Ошибка при получении статистики: {e}")
    
    with tab2:
        display_taxpayer_search_tab()


if __name__ == "__main__":
    main()
