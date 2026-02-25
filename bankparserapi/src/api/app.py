#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FastAPI application for bank statement parsing API.

Endpoints:
- POST /api/upload_initial - Upload statements and create project
- POST /api/get_ids_by_iin - Get project IDs by IIN
- POST /api/get_analytics - Get analytics by project ID
- POST /api/get_source_data - Get source files as ZIP by project ID
- POST /api/refresh - Refresh authentication token
"""

from __future__ import annotations

from pathlib import Path
import sys
import json
from datetime import datetime, date
from typing import List, Optional, Dict, Any
import uuid
import zipfile
import io
import base64
import pandas as pd

from fastapi import FastAPI, HTTPException, Depends, Header, Security, UploadFile, File
from fastapi.responses import Response, FileResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, EmailStr
from sqlalchemy.orm import Session
import uvicorn

# --- ensure project root on sys.path ---
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.api.storage import get_storage
from src.api.statement_processor import StatementProcessor
from src.api.taxpayer_api import TaxpayerAPIClient, TaxpayerType
from src.core.analysis import get_last_full_12m_window, compute_ip_income_for_statement
from src.api.auth import (
    get_db, create_user, authenticate_user, get_current_user_from_token,
    create_access_token, User, get_user_by_login, get_user_by_email
)
from src.db.database import DatabaseConnection, import_statement_to_db
from src.db.config import DB_CONFIG

app = FastAPI(
    title="Bank Statement Parser API",
    description="API for parsing bank statements and generating analytics",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allow all methods
    allow_headers=["*"],  # Allow all headers
)


# ==================== Pydantic Models ====================

class StatementRequest(BaseModel):
    """Statement file data in request"""
    id: str = Field(..., description="Unique statement identifier")
    name: str = Field(..., description="Statement file name")
    extension: str = Field(..., description="File extension (e.g., '.pdf')")
    data: str = Field(..., description="Base64 encoded file content")


class UploadInitialRequest(BaseModel):
    """Request model for /api/upload_initial"""
    iin: str = Field(..., description="IIN/BIN of the client")
    statements: List[StatementRequest] = Field(..., description="List of statement files")


class StatementResponse(BaseModel):
    """Statement response in upload_initial response"""
    id: str
    name: str
    extension: str
    status: int = Field(..., description="Status code (see status classifier)")
    message: str = Field(..., description="Status message")


class AnalyticsResponse(BaseModel):
    """Analytics data structure"""
    iin: Optional[str] = None
    registration_date: Optional[str] = None
    average_income: Optional[float] = None


class UploadInitialResponse(BaseModel):
    """Response model for /api/upload_initial"""
    status: int = Field(..., description="0=Success, 1=Failure, 2=Data mismatch")
    message: str
    project_id: int
    iin: str
    create_date: str = Field(..., description="Format: DD.MM.YYYY HH:MM:SS")
    analytics: Dict[str, Any] = Field(default_factory=dict)
    statements: List[StatementResponse]


class GetIdsByIinRequest(BaseModel):
    """Request model for /api/get_ids_by_iin"""
    iin: str


class ProjectData(BaseModel):
    """Project data structure"""
    project_id: int
    upload_date: str = Field(..., description="Format: DD-MM-YYYY")
    status: int


class GetIdsByIinResponse(BaseModel):
    """Response model for /api/get_ids_by_iin"""
    projects_data: List[ProjectData]


class GetAnalyticsRequest(BaseModel):
    """Request model for /api/get_analytics"""
    project_id: int


class GetAnalyticsResponse(BaseModel):
    """Response model for /api/get_analytics"""
    status: Optional[int] = None
    analytics: Dict[str, Any] = Field(default_factory=dict)


class GetSourceDataRequest(BaseModel):
    """Request model for /api/get_source_data"""
    project_id: int


class RefreshRequest(BaseModel):
    """Request model for /api/refresh"""
    login: str
    password: str


class RefreshResponse(BaseModel):
    """Response model for /api/refresh"""
    token: str


class CreateProjectRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)


class CreateProjectResponse(BaseModel):
    project_id: str
    name: str
    status: str
    created_at: str


class ProjectResponse(BaseModel):
    project_id: str
    name: str
    status: str
    statements_count: int
    created_at: str


class ProjectUploadItem(BaseModel):
    source_filename: str
    bank: Optional[str] = None
    statement_id: Optional[str] = None
    processing_status: str
    processing_message: str


class ProjectUploadResponse(BaseModel):
    project_id: str
    total_files: int
    processed: int
    skipped: int
    failed: int
    items: List[ProjectUploadItem]


class RegisterRequest(BaseModel):
    """Request model for /api/register"""
    login: str = Field(..., min_length=3, max_length=50)
    password: str = Field(..., min_length=6)
    email: Optional[EmailStr] = None


class RegisterResponse(BaseModel):
    """Response model for /api/register"""
    message: str
    login: str


class TaxpayerSearchRequest(BaseModel):
    """Request model for /api/taxpayer/search"""
    portal_host: str = Field(..., description="Базовый URL портала (например, https://portal.example.com)")
    taxpayer_code: str = Field(..., description="ИИН/БИН налогоплательщика (12 цифр)", min_length=12, max_length=12)
    taxpayer_type: str = Field(..., description="Тип налогоплательщика: IP, LZCHP, UL")
    name: Optional[str] = Field(None, description="Наименование (для ИП и ЮЛ)")
    first_name: Optional[str] = Field(None, description="Имя (для ЛЗЧП)")
    last_name: Optional[str] = Field(None, description="Фамилия (для ЛЗЧП)")
    print: bool = Field(False, description="Если true, возвращает PDF в base64, иначе JSON")


class TaxpayerSearchResponse(BaseModel):
    """Response model for /api/taxpayer/search"""
    success: bool
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    status_code: Optional[int] = None
    message: Optional[str] = None
    pdf_base64: Optional[str] = None


# ==================== Authentication ====================

security = HTTPBearer()


async def verify_token(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db)
) -> User:
    """
    Verify authorization token from header and return current user.
    """
    token = credentials.credentials
    user = get_current_user_from_token(token, db)
    if user is None:
        raise HTTPException(
            status_code=401,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if not user.is_active:
        raise HTTPException(status_code=403, detail="User is inactive")
    return user


# ==================== Helper Functions ====================

def format_datetime(dt: datetime) -> str:
    """Format datetime as DD.MM.YYYY HH:MM:SS"""
    return dt.strftime("%d.%m.%Y %H:%M:%S")


def format_date(d: date) -> str:
    """Format date as DD-MM-YYYY"""
    return d.strftime("%d-%m-%Y")


def _normalize_calc_window_for_ui(statement_date: date, mode: str = "test") -> tuple[date, date]:
    """
    Return calculation window boundaries.
    - test: end date = statement_date
    - prod: end date = last day of previous month
    Start date is first day of month 11 months before end month start.
    """
    if mode == "prod":
        first_day_curr_month = statement_date.replace(day=1)
        calc_end = first_day_curr_month - date.resolution
    else:
        calc_end = statement_date

    end_month_start = calc_end.replace(day=1)
    month_index = end_month_start.year * 12 + end_month_start.month - 1 - 11
    start_year = month_index // 12
    start_month = month_index % 12 + 1
    calc_start = date(start_year, start_month, 1)
    return calc_start, calc_end


def _build_monthly_income_df(enriched_df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    if enriched_df is None or enriched_df.empty:
        return None
    if "txn_date" not in enriched_df.columns or "ip_credit_amount" not in enriched_df.columns:
        return None
    work = enriched_df.copy()
    work["txn_date"] = pd.to_datetime(work["txn_date"], errors="coerce")
    work = work[work["txn_date"].notna()]
    if "ip_is_business_income" in work.columns:
        work = work[work["ip_is_business_income"].fillna(False).astype(bool)]
    if work.empty:
        return pd.DataFrame(columns=["month", "business_income", "transaction_count"])
    work["month"] = work["txn_date"].dt.to_period("M").astype(str)
    return (
        work.groupby("month", as_index=False)
        .agg(
            business_income=("ip_credit_amount", "sum"),
            transaction_count=("ip_credit_amount", "count"),
        )
    )


def _ensure_runtime_schema() -> None:
    db = DatabaseConnection(**DB_CONFIG)
    db.connect()
    try:
        db.ensure_project_schema()
    finally:
        db.disconnect()


@app.on_event("startup")
async def startup_schema_init():
    """Ensure runtime schema additions exist for project workflow."""
    try:
        _ensure_runtime_schema()
    except Exception as e:
        print(f"⚠️ Runtime schema init failed: {e}")


# ==================== Global Instances ====================

storage = get_storage()
processor = StatementProcessor()


def _create_project_db(name: str, created_by: Optional[str]) -> Dict[str, Any]:
    db = DatabaseConnection(**DB_CONFIG)
    db.connect()
    try:
        project_id = db.execute_insert(
            """
            INSERT INTO projects (name, status, created_by)
            VALUES (%s, 'draft', %s)
            RETURNING id
            """,
            (name, created_by),
        )
        row = db.execute_query(
            "SELECT id, name, status, created_at FROM projects WHERE id = %s",
            (project_id,),
        )[0]
        return row
    finally:
        db.disconnect()


def _get_project_db(project_id: str) -> Optional[Dict[str, Any]]:
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
            WHERE p.id = %s
            GROUP BY p.id, p.name, p.status, p.created_at
            """,
            (project_id,),
        )
        return rows[0] if rows else None
    finally:
        db.disconnect()


def _list_projects_db() -> List[Dict[str, Any]]:
    db = DatabaseConnection(**DB_CONFIG)
    db.connect()
    try:
        return db.execute_query(
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
        db.execute_command("UPDATE projects SET status = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s", (status, project_id))
    finally:
        db.disconnect()


# ==================== API Endpoints ====================

@app.post("/api/register", response_model=RegisterResponse)
async def register(
    request: RegisterRequest,
    db: Session = Depends(get_db)
):
    """
    Register a new user.
    """
    # Check if user already exists
    if get_user_by_login(db, request.login):
        raise HTTPException(status_code=400, detail="Login already registered")
    
    if request.email and get_user_by_email(db, request.email):
        raise HTTPException(status_code=400, detail="Email already registered")
    
    # Create user
    user = create_user(db, login=request.login, password=request.password, email=request.email)
    
    return RegisterResponse(
        message="User registered successfully",
        login=user.login
    )


@app.post("/api/projects", response_model=CreateProjectResponse)
async def create_project(
    request: CreateProjectRequest,
):
    row = _create_project_db(name=request.name.strip(), created_by="public")
    return CreateProjectResponse(
        project_id=str(row["id"]),
        name=row["name"],
        status=row["status"],
        created_at=format_datetime(row["created_at"]),
    )


@app.get("/api/projects", response_model=List[ProjectResponse])
async def list_projects(
):
    rows = _list_projects_db()
    return [
        ProjectResponse(
            project_id=str(r["id"]),
            name=r["name"],
            status=r["status"],
            statements_count=int(r.get("statements_count") or 0),
            created_at=format_datetime(r["created_at"]),
        )
        for r in rows
    ]


@app.get("/api/projects/{project_id}", response_model=ProjectResponse)
async def get_project(
    project_id: str,
):
    row = _get_project_db(project_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Project not found")
    return ProjectResponse(
        project_id=str(row["id"]),
        name=row["name"],
        status=row["status"],
        statements_count=int(row.get("statements_count") or 0),
        created_at=format_datetime(row["created_at"]),
    )


@app.get("/api/projects/{project_id}/statements")
async def get_project_statements(
    project_id: str,
):
    project = _get_project_db(project_id)
    if project is None:
        raise HTTPException(status_code=404, detail="Project not found")

    db = DatabaseConnection(**DB_CONFIG)
    db.connect()
    try:
        rows = db.execute_query(
            """
            SELECT
                ps.id,
                ps.upload_order,
                ps.source_filename,
                ps.processing_status,
                ps.processing_message,
                s.id AS statement_id,
                s.bank,
                s.statement_generation_date AS statement_date,
                s.last_operation_date,
                s.first_operation_date,
                s.calc_end_date,
                s.calc_start_date,
                s.uploaded_at
            FROM project_statements ps
            LEFT JOIN statements s ON s.id = ps.statement_id
            WHERE ps.project_id = %s
            ORDER BY ps.upload_order ASC
            """,
            (project_id,),
        )
        return rows
    finally:
        db.disconnect()


@app.get("/api/projects-statements")
async def get_projects_statements(
    project_id: str = None,
):
    db = DatabaseConnection(**DB_CONFIG)
    db.connect()
    try:
        if project_id:
            rows = db.execute_query(
                """
                SELECT
                    p.id AS project_id,
                    p.name AS project_name,
                    p.status AS project_status,
                    ps.upload_order,
                    ps.source_filename,
                    ps.processing_status,
                    ps.processing_message,
                    s.id AS statement_id,
                    s.bank,
                    s.statement_generation_date AS statement_date,
                    s.last_operation_date,
                    s.first_operation_date,
                    s.calc_end_date,
                    s.calc_start_date,
                    s.uploaded_at
                FROM project_statements ps
                JOIN projects p ON p.id = ps.project_id
                LEFT JOIN statements s ON s.id = ps.statement_id
                WHERE p.id = %s
                ORDER BY ps.upload_order ASC
                """,
                (project_id,),
            )
        else:
            rows = db.execute_query(
                """
                SELECT
                    p.id AS project_id,
                    p.name AS project_name,
                    p.status AS project_status,
                    ps.upload_order,
                    ps.source_filename,
                    ps.processing_status,
                    ps.processing_message,
                    s.id AS statement_id,
                    s.bank,
                    s.statement_generation_date AS statement_date,
                    s.last_operation_date,
                    s.first_operation_date,
                    s.calc_end_date,
                    s.calc_start_date,
                    s.uploaded_at
                FROM project_statements ps
                JOIN projects p ON p.id = ps.project_id
                LEFT JOIN statements s ON s.id = ps.statement_id
                ORDER BY p.created_at DESC, ps.upload_order ASC
                """
            )
        return rows
    finally:
        db.disconnect()


@app.get("/api/projects-overview")
async def get_projects_overview(
    project_id: str = None,
):
    db = DatabaseConnection(**DB_CONFIG)
    db.connect()
    try:
        if project_id:
            rows = db.execute_query(
                """
                SELECT
                    p.id AS project_id,
                    p.name AS project_name,
                    p.status AS project_status,
                    p.created_at,
                    COUNT(ps.id) AS total_files,
                    COUNT(*) FILTER (WHERE ps.processing_status = 'success') AS success_files,
                    COUNT(*) FILTER (WHERE ps.processing_status = 'skipped') AS skipped_files,
                    COUNT(*) FILTER (WHERE ps.processing_status = 'error') AS error_files
                FROM projects p
                LEFT JOIN project_statements ps ON ps.project_id = p.id
                WHERE p.id = %s
                GROUP BY p.id, p.name, p.status, p.created_at
                ORDER BY p.created_at DESC
                """,
                (project_id,),
            )
        else:
            rows = db.execute_query(
                """
                SELECT
                    p.id AS project_id,
                    p.name AS project_name,
                    p.status AS project_status,
                    p.created_at,
                    COUNT(ps.id) AS total_files,
                    COUNT(*) FILTER (WHERE ps.processing_status = 'success') AS success_files,
                    COUNT(*) FILTER (WHERE ps.processing_status = 'skipped') AS skipped_files,
                    COUNT(*) FILTER (WHERE ps.processing_status = 'error') AS error_files
                FROM projects p
                LEFT JOIN project_statements ps ON ps.project_id = p.id
                GROUP BY p.id, p.name, p.status, p.created_at
                ORDER BY p.created_at DESC
                """
            )
        return rows
    finally:
        db.disconnect()


@app.get("/api/projects/{project_id}/tx_ip")
async def get_project_tx_ip(
    project_id: str,
):
    project = _get_project_db(project_id)
    if project is None:
        raise HTTPException(status_code=404, detail="Project not found")

    db = DatabaseConnection(**DB_CONFIG)
    db.connect()
    try:
        rows = db.execute_query(
            """
            SELECT
                t.statement_id,
                t.operation_date AS txn_date,
                t.document_number,
                t.debit_amount,
                t.credit_amount,
                t.counterparty_name,
                t.counterparty_iin_bin,
                t.payment_purpose,
                t.payment_code_knp AS knp,
                tif.knp_normalized AS ip_knp_norm,
                tif.is_non_business_by_knp AS ip_is_non_business_by_knp,
                tif.is_non_business_by_keywords AS ip_is_non_business_by_keywords,
                tif.is_non_business AS ip_is_non_business,
                tif.ip_credit_amount,
                tif.is_business_income AS ip_is_business_income
            FROM transactions t
            JOIN project_statements ps ON ps.statement_id = t.statement_id
            LEFT JOIN transactions_ip_flags tif ON tif.transaction_id = t.id
            WHERE ps.project_id = %s
            ORDER BY t.operation_date DESC, t.created_at DESC
            """,
            (project_id,),
        )
        return rows
    finally:
        db.disconnect()


@app.post("/api/projects/{project_id}/upload", response_model=ProjectUploadResponse)
async def upload_project_statements(
    project_id: str,
    files: List[UploadFile] = File(...),
):
    project = _get_project_db(project_id)
    if project is None:
        raise HTTPException(status_code=404, detail="Project not found")

    existing_count = _count_project_statements(project_id)
    if existing_count + len(files) > 9:
        raise HTTPException(status_code=400, detail=f"Project supports up to 9 statements. Existing: {existing_count}")

    items: List[ProjectUploadItem] = []
    processed = 0
    skipped = 0
    failed = 0

    _update_project_status(project_id, "processing")

    for idx, upl in enumerate(files, start=1):
        source_filename = upl.filename or f"statement_{idx}.pdf"
        file_bytes = await upl.read()
        statement_id = str(uuid.uuid4())
        b64 = base64.b64encode(file_bytes).decode("utf-8")

        parse_result = processor.parse_statement_base64(
            statement_id=statement_id,
            statement_name=source_filename,
            extension=".pdf",
            base64_data=b64,
            expected_iin=None,
        )

        parsed_statement = parse_result.get("parsed_statement")
        status_code = parse_result.get("status")

        if not parsed_statement or status_code != processor.STATUS_SUCCESS:
            message = parse_result.get("message", "Parse failed")
            _link_statement_to_project(
                project_id=project_id,
                statement_id=None,
                upload_order=existing_count + idx,
                source_filename=source_filename,
                processing_status="error",
                processing_message=message,
            )
            failed += 1
            items.append(ProjectUploadItem(
                source_filename=source_filename,
                bank=None,
                statement_id=None,
                processing_status="error",
                processing_message=message,
            ))
            continue

        iin = (getattr(parsed_statement, "iin_bin", None) or "").strip()
        if not iin:
            msg = "Skipped: no IIN/BIN/INN data for IP calculation"
            _link_statement_to_project(
                project_id=project_id,
                statement_id=None,
                upload_order=existing_count + idx,
                source_filename=source_filename,
                processing_status="skipped",
                processing_message=msg,
            )
            skipped += 1
            items.append(ProjectUploadItem(
                source_filename=source_filename,
                bank=getattr(parsed_statement, "bank", None),
                statement_id=None,
                processing_status="skipped",
                processing_message=msg,
            ))
            continue

        statement_date = (
            getattr(parsed_statement, "statement_generation_date", None)
            or getattr(parsed_statement, "period_to", None)
            or date.today()
        )
        if isinstance(statement_date, datetime):
            statement_date = statement_date.date()
        if isinstance(statement_date, pd.Timestamp):
            statement_date = statement_date.date()
        if not isinstance(statement_date, date):
            statement_date = date.today()

        window_start, window_end = get_last_full_12m_window(statement_date)
        enriched_df, income_summary = compute_ip_income_for_statement(parsed_statement, window_start, window_end)
        monthly_df = _build_monthly_income_df(enriched_df)

        statement_data = {
            "header_df": getattr(parsed_statement, "header_df", None),
            "tx_df": getattr(parsed_statement, "tx_df", None),
            "footer_df": getattr(parsed_statement, "footer_df", None),
            "meta_df": getattr(parsed_statement, "meta_df", None),
            "tx_ip_df": enriched_df,
            "monthly_income_df": monthly_df,
            "income_summary": income_summary if income_summary else {},
            "client_iin": iin,
            "client_name": getattr(parsed_statement, "account_holder_name", None),
            "account_number": getattr(parsed_statement, "account_number", None),
            "pdf_name": source_filename,
        }

        try:
            db = DatabaseConnection(**DB_CONFIG)
            db.connect()
            db_statement_id = import_statement_to_db(db, statement_data, getattr(parsed_statement, "bank", "Unknown"))
            db.disconnect()

            _link_statement_to_project(
                project_id=project_id,
                statement_id=db_statement_id,
                upload_order=existing_count + idx,
                source_filename=source_filename,
                processing_status="success",
                processing_message="Processed successfully",
            )
            processed += 1
            items.append(ProjectUploadItem(
                source_filename=source_filename,
                bank=getattr(parsed_statement, "bank", None),
                statement_id=str(db_statement_id),
                processing_status="success",
                processing_message="Processed successfully",
            ))
        except Exception as e:
            _link_statement_to_project(
                project_id=project_id,
                statement_id=None,
                upload_order=existing_count + idx,
                source_filename=source_filename,
                processing_status="error",
                processing_message=f"DB save failed: {e}",
            )
            failed += 1
            items.append(ProjectUploadItem(
                source_filename=source_filename,
                bank=getattr(parsed_statement, "bank", None),
                statement_id=None,
                processing_status="error",
                processing_message=f"DB save failed: {e}",
            ))

    if failed > 0 and processed == 0:
        _update_project_status(project_id, "failed")
    elif failed > 0 or skipped > 0:
        _update_project_status(project_id, "completed_with_warnings")
    else:
        _update_project_status(project_id, "completed")

    return ProjectUploadResponse(
        project_id=project_id,
        total_files=len(files),
        processed=processed,
        skipped=skipped,
        failed=failed,
        items=items,
    )


# ==================== Database Helper Functions ====================

def save_statement_to_db(parsed_statement, tx_df, footer_df, meta_df, tx_ip_df, monthly_income_df, income_summary_df, bank: str):
    """
    Save parsed statement to PostgreSQL database.
    
    Args:
        parsed_statement: Statement object from parser
        tx_df, footer_df, meta_df, tx_ip_df, monthly_income_df, income_summary_df: DataFrames
        bank: Bank name
    """
    try:
        db = DatabaseConnection(**DB_CONFIG)
        db.connect()
        
        statement_data = {
            'header_df': parsed_statement.header_df if hasattr(parsed_statement, 'header_df') else None,
            'tx_df': tx_df,
            'footer_df': footer_df,
            'meta_df': meta_df,
            'tx_ip_df': tx_ip_df,
            'monthly_income_df': monthly_income_df,
            'income_summary': income_summary_df.iloc[0].to_dict() if income_summary_df is not None and len(income_summary_df) > 0 else {},
            'client_iin': parsed_statement.iin_bin if hasattr(parsed_statement, 'iin_bin') else None,
            'client_name': parsed_statement.account_holder_name if hasattr(parsed_statement, 'account_holder_name') else None,
            'account_number': parsed_statement.account_number if hasattr(parsed_statement, 'account_number') else None,
            'pdf_name': parsed_statement.pdf_name if hasattr(parsed_statement, 'pdf_name') else 'unknown.pdf',
        }
        
        statement_id = import_statement_to_db(db, statement_data, bank)
        db.disconnect()
        
        return statement_id
    except Exception as e:
        print(f"⚠️ Error saving to database: {e}")
        return None


@app.post("/api/upload_initial", response_model=UploadInitialResponse)
async def upload_initial(
    request: UploadInitialRequest,
    token: HTTPAuthorizationCredentials = Security(security),
    current_user: User = Depends(verify_token)
):
    """
    Upload bank statements and create a new project.
    
    Processes statements, creates project, returns analytics.
    """
    try:
        # Process all statements
        statements_resp = []
        parsed_statements = []
        statement_files_data = []  # Store (statement_id, pdf_bytes, filename) for later saving
        has_data_mismatch = False
        has_failure = False
        
        for stmt_req in request.statements:
            # Parse statement
            result = processor.parse_statement_base64(
                statement_id=stmt_req.id,
                statement_name=stmt_req.name,
                extension=stmt_req.extension,
                base64_data=stmt_req.data,
                expected_iin=request.iin
            )
            
            # Track status
            if result['status'] == processor.STATUS_DATA_MISMATCH:
                has_data_mismatch = True
            elif result['status'] == processor.STATUS_FAILURE or result['status'] == processor.STATUS_SCANNED_COPY:
                has_failure = True
            
            # Store file data for later saving
            pdf_bytes = base64.b64decode(stmt_req.data)
            # Ensure filename includes extension so storage glob("*.*") picks it up
            filename = stmt_req.name
            ext = stmt_req.extension or ""
            if ext and not filename.endswith(ext):
                filename = f"{filename}{ext}"
            statement_files_data.append((stmt_req.id, pdf_bytes, filename))
            
            # Store parsed statement for analytics
            if result['parsed_statement']:
                parsed_statements.append(result['parsed_statement'])
                
                # Try to save to database if parsing was successful
                if result['status'] == processor.STATUS_SUCCESS:
                    try:
                        bank_name = result.get('bank', 'Unknown')
                        tx_df = result.get('tx_df')
                        footer_df = result.get('footer_df')
                        meta_df = result.get('meta_df')
                        tx_ip_df = result.get('tx_ip_df')
                        monthly_income_df = result.get('monthly_income_df')
                        income_summary_df = result.get('income_summary_df')
                        
                        db_id = save_statement_to_db(
                            result['parsed_statement'],
                            tx_df,
                            footer_df,
                            meta_df,
                            tx_ip_df,
                            monthly_income_df,
                            income_summary_df,
                            bank_name
                        )
                        print(f"✓ Statement {stmt_req.id} saved to PostgreSQL: {db_id}")
                    except Exception as e:
                        print(f"⚠️ Could not save {stmt_req.id} to PostgreSQL: {e}")
            
            # Add to response
            statements_resp.append(StatementResponse(
                id=result['id'],
                name=result['name'],
                extension=result['extension'],
                status=result['status'],
                message=result['message']
            ))
        
        # Calculate analytics from successfully parsed statements
        analytics = {}
        if parsed_statements:
            analytics = processor.calculate_analytics(parsed_statements)
        
        # Determine overall project status
        if has_data_mismatch:
            project_status = 2  # Data mismatch
            response_message = "Расхождение регистрационных данных"
        elif has_failure:
            project_status = 1  # Failure
            response_message = "Провал"
        else:
            project_status = 0  # Success
            response_message = "Успех"
        
        # Create project
        project = storage.create_project(
            iin=request.iin,
            statements=[{
                'id': stmt.id,
                'name': stmt.name,
                'extension': stmt.extension,
                'status': stmt.status,
                'message': stmt.message
            } for stmt in statements_resp],
            analytics=analytics,
            status=project_status
        )
        
        # Save statement files after project creation
        for statement_id, pdf_bytes, filename in statement_files_data:
            storage.save_statement_file(
                project_id=project.project_id,
                statement_id=statement_id,
                file_data=pdf_bytes,
                filename=filename
            )
        
        return UploadInitialResponse(
            status=project_status,
            message=response_message,
            project_id=project.project_id,
            iin=request.iin,
            create_date=format_datetime(project.create_date),
            analytics=analytics,
            statements=statements_resp
        )
    
    except Exception as e:
        # According to documentation, always return 200, but with error status in body
        # Return error response with status 1 (Failure) in body
        return UploadInitialResponse(
            status=1,
            message=f"Провал: {str(e)}",
            project_id=0,
            iin=request.iin,
            create_date=format_datetime(datetime.now()),
            analytics={},
            statements=[]
        )


@app.post("/api/get_ids_by_iin", response_model=GetIdsByIinResponse)
async def get_ids_by_iin(
    request: GetIdsByIinRequest,
    token: HTTPAuthorizationCredentials = Security(security),
    current_user: User = Depends(verify_token)
):
    """
    Get list of all project IDs created for given IIN.
    """
    try:
        projects = storage.get_projects_by_iin(request.iin)
        
        projects_data = [
            ProjectData(
                project_id=p.project_id,
                upload_date=format_date(p.create_date.date()),
                status=p.status
            )
            for p in projects
        ]
        
        return GetIdsByIinResponse(projects_data=projects_data)
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/get_analytics", response_model=GetAnalyticsResponse)
async def get_analytics(
    request: GetAnalyticsRequest,
    token: HTTPAuthorizationCredentials = Security(security),
    current_user: User = Depends(verify_token)
):
    """
    Get analytics data for existing project.
    """
    try:
        project = storage.get_project(request.project_id)
        
        if project is None:
            raise HTTPException(status_code=404, detail=f"Project {request.project_id} not found")
        
        # According to documentation:
        # Scenario 2: returns analytics without status (just analytics dict)
        # Scenario 3: returns analytics with status=1
        # We'll return with status=1 for scenario 3 (requesting analytics for existing project)
        analytics = project.analytics.copy() if project.analytics else {}
        
        # Map 'iin' to 'id' if present (according to scenario 2 example)
        if 'iin' in analytics and 'id' not in analytics:
            analytics['id'] = analytics['iin']
        
        # Return with status=1 for existing project (scenario 3)
        return GetAnalyticsResponse(
            status=1,
            analytics=analytics
        )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/get_source_data")
async def get_source_data(
    request: GetSourceDataRequest,
    token: HTTPAuthorizationCredentials = Security(security),
    current_user: User = Depends(verify_token)
):
    """
    Get source statement files as ZIP archive for existing project.
    """
    try:
        project = storage.get_project(request.project_id)
        
        if project is None:
            raise HTTPException(status_code=404, detail=f"Project {request.project_id} not found")
        
        # Get all statement files for the project
        statement_files = storage.get_statement_files(request.project_id)
        
        # Create ZIP archive
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for file_path in statement_files:
                # Extract original filename (remove statement_id prefix)
                original_name = file_path.name
                # Try to get original name from project statements
                for stmt in project.statements:
                    if isinstance(stmt, dict) and stmt.get('id') and stmt['id'] in original_name:
                        # Use original filename with extension
                        stmt_name = stmt.get('name', file_path.name)
                        stmt_ext = stmt.get('extension', '')
                        if stmt_ext and not stmt_name.endswith(stmt_ext):
                            original_name = stmt_name + stmt_ext
                        else:
                            original_name = stmt_name
                        break
                
                zip_file.write(str(file_path), arcname=original_name)
        
        zip_buffer.seek(0)
        
        return Response(
            content=zip_buffer.getvalue(),
            media_type="application/zip",
            headers={"Content-Disposition": f"attachment; filename=project_{request.project_id}_statements.zip"}
        )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/refresh", response_model=RefreshResponse)
async def refresh_token(
    request: RefreshRequest,
    db: Session = Depends(get_db)
):
    """
    Refresh authentication token using login and password.
    Returns a new JWT access token.
    """
    # Authenticate user
    user = authenticate_user(db, request.login, request.password)
    if user is None:
        raise HTTPException(
            status_code=401,
            detail="Invalid login or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Create access token
    access_token = create_access_token(data={"sub": user.login})
    
    return RefreshResponse(token=access_token)


@app.post("/api/login", response_model=RefreshResponse)
async def login(
    request: RefreshRequest,
    db: Session = Depends(get_db)
):
    """
    Login endpoint — authenticate user and return JWT access token.
    This mirrors `/api/refresh` and is provided so Swagger/clients
    can discover a conventional `login` operation.
    """
    user = authenticate_user(db, request.login, request.password)
    if user is None:
        raise HTTPException(
            status_code=401,
            detail="Invalid login or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    access_token = create_access_token(data={"sub": user.login})

    return RefreshResponse(token=access_token)


@app.post("/api/encode_file")
async def encode_file(file: UploadFile = File(...)):
    """
    Accept a file upload and return its base64-encoded content.

    Useful to prepare the `data` field for `/api/upload_initial`.
    """
    try:
        content = await file.read()
        encoded = base64.b64encode(content).decode("utf-8")
        return {"filename": file.filename, "base64": encoded}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/taxpayer/search", response_model=TaxpayerSearchResponse)
async def search_taxpayer(
    request: TaxpayerSearchRequest,
    x_portal_token: str = Header(..., alias="X-Portal-Token", description="Токен доступа к порталу")
):
    """
    Поиск налогоплательщика через API сервиса «Поиск Налогоплательщика».
    
    Поддерживает три типа налогоплательщиков:
    - IP (Индивидуальный предприниматель): требуется taxpayer_code и name
    - LZCHP (Лицо, занимающееся частной практикой): требуется taxpayer_code, first_name, last_name
    - UL (Юридическое лицо): требуется taxpayer_code и name
    """
    try:
        # Валидация типа налогоплательщика
        try:
            taxpayer_type = TaxpayerType(request.taxpayer_type.upper())
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Неверный тип налогоплательщика: {request.taxpayer_type}. Допустимые значения: IP, LZCHP, UL"
            )
        
        # Создание клиента API
        client = TaxpayerAPIClient(
            portal_host=request.portal_host,
            portal_token=x_portal_token
        )
        
        # Выполнение поиска
        result = client.search_taxpayer(
            taxpayer_code=request.taxpayer_code,
            taxpayer_type=taxpayer_type,
            name=request.name,
            first_name=request.first_name,
            last_name=request.last_name,
            print=request.print
        )
        
        # Формирование ответа
        if result.get("success"):
            if request.print:
                return TaxpayerSearchResponse(
                    success=True,
                    pdf_base64=result.get("pdf_base64"),
                    status_code=result.get("status_code", 200)
                )
            else:
                return TaxpayerSearchResponse(
                    success=True,
                    data=result.get("data"),
                    status_code=result.get("status_code", 200)
                )
        else:
            return TaxpayerSearchResponse(
                success=False,
                error=result.get("error"),
                status_code=result.get("status_code"),
                message=result.get("message"),
                data=result.get("details")
            )
    
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при поиске налогоплательщика: {str(e)}")


# ==================== Database View Endpoints ====================

@app.get("/api/db/statements")
async def get_statements(project_id: str = None):
    """Get all statements from database"""
    try:
        db = DatabaseConnection(**DB_CONFIG)
        db.connect()

        if project_id:
            results = db.execute_query("""
                SELECT
                    s.id,
                    s.bank,
                    s.pdf_name,
                    s.period_from,
                    s.period_to,
                    s.statement_generation_date as statement_date,
                    s.last_operation_date,
                    s.first_operation_date,
                    s.calc_end_date,
                    s.calc_start_date,
                    s.uploaded_at,
                    c.full_name as client_name,
                    c.iin_bin,
                    a.account_number,
                    ps.processing_status,
                    ps.processing_message,
                    COUNT(t.id) as transaction_count,
                    s.created_at
                FROM statements s
                JOIN project_statements ps ON ps.statement_id = s.id
                LEFT JOIN accounts a ON s.account_id = a.id
                LEFT JOIN clients c ON a.client_id = c.id
                LEFT JOIN transactions t ON s.id = t.statement_id
                WHERE ps.project_id = %s
                GROUP BY
                    s.id, s.bank, s.pdf_name, s.period_from, s.period_to,
                    s.statement_generation_date, s.last_operation_date, s.first_operation_date,
                    s.calc_end_date, s.calc_start_date, s.uploaded_at,
                    c.full_name, c.iin_bin, a.account_number,
                    ps.processing_status, ps.processing_message, s.created_at
                ORDER BY s.created_at DESC
            """, (project_id,))
        else:
            results = db.execute_query("""
                SELECT
                    s.id,
                    s.bank,
                    s.pdf_name,
                    s.period_from,
                    s.period_to,
                    s.statement_generation_date as statement_date,
                    s.last_operation_date,
                    s.first_operation_date,
                    s.calc_end_date,
                    s.calc_start_date,
                    s.uploaded_at,
                    c.full_name as client_name,
                    c.iin_bin,
                    a.account_number,
                    COUNT(t.id) as transaction_count,
                    s.created_at
                FROM statements s
                LEFT JOIN accounts a ON s.account_id = a.id
                LEFT JOIN clients c ON a.client_id = c.id
                LEFT JOIN transactions t ON s.id = t.statement_id
                GROUP BY
                    s.id, s.bank, s.pdf_name, s.period_from, s.period_to,
                    s.statement_generation_date, s.last_operation_date, s.first_operation_date,
                    s.calc_end_date, s.calc_start_date, s.uploaded_at,
                    c.full_name, c.iin_bin, a.account_number, s.created_at
                ORDER BY s.created_at DESC
            """)
        
        db.disconnect()
        return [dict(row) for row in results] if results else []
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/db/transactions")
async def get_transactions(statement_id: str = None, project_id: str = None):
    """Get transactions, optionally filtered by statement_id"""
    try:
        db = DatabaseConnection(**DB_CONFIG)
        db.connect()
        
        if statement_id:
            results = db.execute_query("""
                SELECT 
                    id,
                    statement_id,
                    operation_date,
                    credit_amount,
                    debit_amount,
                    payment_code_knp,
                    payment_purpose,
                    counterparty_name,
                    counterparty_iin_bin,
                    document_number,
                    created_at
                FROM transactions
                WHERE statement_id = %s
                ORDER BY operation_date DESC
            """, (statement_id,))
        elif project_id:
            results = db.execute_query("""
                SELECT
                    t.id,
                    t.statement_id,
                    t.operation_date,
                    t.credit_amount,
                    t.debit_amount,
                    t.payment_code_knp,
                    t.payment_purpose,
                    t.counterparty_name,
                    t.counterparty_iin_bin,
                    t.document_number,
                    t.created_at
                FROM transactions t
                JOIN project_statements ps ON ps.statement_id = t.statement_id
                WHERE ps.project_id = %s
                ORDER BY t.operation_date DESC
                LIMIT 500
            """, (project_id,))
        else:
            results = db.execute_query("""
                SELECT 
                    id,
                    statement_id,
                    operation_date,
                    credit_amount,
                    debit_amount,
                    payment_code_knp,
                    payment_purpose,
                    counterparty_name,
                    counterparty_iin_bin,
                    document_number,
                    created_at
                FROM transactions
                ORDER BY operation_date DESC
                LIMIT 100
            """)
        
        db.disconnect()
        return [dict(row) for row in results] if results else []
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/db/clients")
async def get_clients(project_id: str = None):
    """Get all clients"""
    try:
        db = DatabaseConnection(**DB_CONFIG)
        db.connect()
        
        if project_id:
            results = db.execute_query("""
                SELECT
                    c.id,
                    c.iin_bin,
                    c.full_name,
                    c.client_type,
                    COUNT(DISTINCT a.id) as account_count,
                    COUNT(DISTINCT s.id) as statement_count,
                    c.created_at
                FROM clients c
                LEFT JOIN accounts a ON c.id = a.client_id
                LEFT JOIN statements s ON a.id = s.account_id
                LEFT JOIN project_statements ps ON ps.statement_id = s.id
                WHERE ps.project_id = %s
                GROUP BY c.id, c.iin_bin, c.full_name, c.client_type, c.created_at
                ORDER BY c.created_at DESC
            """, (project_id,))
        else:
            results = db.execute_query("""
                SELECT
                    c.id,
                    c.iin_bin,
                    c.full_name,
                    c.client_type,
                    COUNT(DISTINCT a.id) as account_count,
                    COUNT(DISTINCT s.id) as statement_count,
                    c.created_at
                FROM clients c
                LEFT JOIN accounts a ON c.id = a.client_id
                LEFT JOIN statements s ON a.id = s.account_id
                GROUP BY c.id, c.iin_bin, c.full_name, c.client_type, c.created_at
                ORDER BY c.created_at DESC
            """)
        
        db.disconnect()
        return [dict(row) for row in results] if results else []
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/db/summary")
async def get_summary(project_id: str = None):
    """Get database summary statistics"""
    try:
        db = DatabaseConnection(**DB_CONFIG)
        db.connect()
        
        summary = {}
        if project_id:
            summary_rows = db.execute_query(
                """
                SELECT
                    COUNT(DISTINCT ps.project_id) AS projects,
                    COUNT(DISTINCT ps.statement_id) AS statements,
                    COUNT(DISTINCT t.id) AS transactions,
                    COUNT(DISTINCT c.id) AS clients
                FROM project_statements ps
                LEFT JOIN statements s ON s.id = ps.statement_id
                LEFT JOIN transactions t ON t.statement_id = s.id
                LEFT JOIN accounts a ON s.account_id = a.id
                LEFT JOIN clients c ON c.id = a.client_id
                WHERE ps.project_id = %s
                """,
                (project_id,),
            )
            row = summary_rows[0] if summary_rows else {}
            summary = {
                "projects": int(row.get("projects") or 0),
                "statements": int(row.get("statements") or 0),
                "transactions": int(row.get("transactions") or 0),
                "clients": int(row.get("clients") or 0),
            }
        else:
            tables = [
                'projects', 'project_statements',
                'clients', 'accounts', 'statements', 'statement_headers',
                'transactions', 'transactions_ip_flags', 'income_summaries',
                'ip_income_monthly', 'statement_footers', 'statement_metadata'
            ]
            for table in tables:
                summary[table] = db.safe_count_table(table)
        
        db.disconnect()
        return summary
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/")
async def root():
    """Root endpoint"""
    return {"message": "Bank Statement Parser API", "version": "1.0.0"}


@app.get("/ui")
async def ui():
    """Database viewer UI"""
    try:
        # Try multiple possible paths (relative to project structure)
        possible_paths = [
            Path(__file__).parent / "ui" / "db_viewer.html",
            PROJECT_ROOT / "src" / "ui" / "db_viewer.html",
        ]

        from src.utils.path_security import validate_path
        for ui_file in possible_paths:
            # Security: Validate path to prevent path traversal
            try:
                resolved_path = ui_file.resolve()
                if resolved_path.exists() and resolved_path.is_file():
                    validated = validate_path(resolved_path, PROJECT_ROOT)
                    with open(validated, 'r', encoding='utf-8') as f:
                        return Response(content=f.read(), media_type="text/html")
            except (OSError, ValueError):
                continue
        
        # If file not found, return error (avoid exposing internal paths)
        return Response(
            content="UI file not found. Ensure src/ui/db_viewer.html exists in the project.",
            status_code=404,
            media_type="text/plain"
        )
    except Exception as e:
        return Response(content=f"Error: {str(e)}", status_code=500, media_type="text/plain")


@app.get("/health")
async def health():
    """Legacy health check (use /livez or /readyz for K8s probes)"""
    return {"status": "ok"}


@app.get("/livez")
async def livez():
    """
    Liveness probe - process is alive.
    Returns 200 if the app is running. K8s restarts container if this fails.
    """
    return {"status": "ok"}


@app.get("/readyz")
async def readyz():
    """
    Readiness probe - app is ready to accept traffic.
    Checks: auth DB (SQLite), storage dir. Optionally PostgreSQL.
    Returns 503 if critical dependencies are unhealthy.
    """
    from sqlalchemy import text
    from src.api.auth import SessionLocal

    checks: Dict[str, str] = {}
    db_ok = False
    storage_ok = False

    # 1) Auth DB (SQLite)
    db = None
    try:
        db = SessionLocal()
        db.execute(text("SELECT 1"))
        checks["auth_db"] = "ok"
        db_ok = True
    except Exception as e:
        checks["auth_db"] = str(e)
    finally:
        if db is not None:
            try:
                db.close()
            except Exception:
                pass

    # 2) Storage dir (projects)
    try:
        storage = get_storage()
        path = storage.base_dir
        if path.exists() and path.is_dir():
            test_file = path / ".health_check_tmp"
            test_file.write_text("")
            test_file.unlink()
            checks["storage"] = "ok"
            storage_ok = True
        else:
            checks["storage"] = "dir missing or not writable"
    except Exception as e:
        checks["storage"] = str(e)

    # 3) Optional: PostgreSQL (bank statements DB) - don't fail ready if PG is down
    try:
        from src.db.database import DatabaseConnection
        with DatabaseConnection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        checks["postgres"] = "ok"
    except Exception as e:
        checks["postgres"] = f"unavailable ({type(e).__name__})"

    all_ok = db_ok and storage_ok
    status_code = 200 if all_ok else 503
    return Response(
        content=json.dumps({"status": "ok" if all_ok else "degraded", "checks": checks}, ensure_ascii=False),
        status_code=status_code,
        media_type="application/json",
    )


if __name__ == "__main__":
    uvicorn.run("src.api.app:app", host="0.0.0.0", port=8000, reload=True)
