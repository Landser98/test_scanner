"""
Unified Database Module for Bank Statements

This module handles all database operations for storing unified bank statements
and provides integration with existing parsers.
"""

import uuid
from datetime import datetime, date
from typing import Optional, Dict, List, Tuple, Any
from dataclasses import dataclass
import json

import psycopg2
from psycopg2 import sql
import pandas as pd

# Bank mapper for unified storage
BANK_NAMES = {
    "Alatau City Bank": "Alatau City Bank",
    "Kaspi Gold": "Kaspi Gold",
    "Kaspi Pay": "Kaspi Pay",
    "Halyk Business": "Halyk Business",
    "Halyk Individual": "Halyk Individual",
    "BCC": "BCC Bank",
    "Eurasian Bank": "Eurasian Bank",
    "Forte Bank": "Forte Bank",
    "Freedom Bank": "Freedom Bank",
}

# Column mappings for transactions from each bank to unified schema
COLUMN_MAPPINGS = {
    "Alatau City Bank": {
        "operation_date": "Дата операции",
        "value_date": "Дата отражения по счету",
        "debit_amount": "Дебет",
        "credit_amount": "Кредит",
        "payment_code_knp": "КНП",
        "payment_purpose": "Назначение платежа",
        "counterparty_name": "Корреспондент",
        "counterparty_iin_bin": "БИН/ИИН",
        "counterparty_bank_bic": "БИК корр.",
        "counterparty_account": "Счет",
        "document_number": "№ док",
    },
    "Kaspi Gold": {
        "operation_date": "date",
        "credit_amount": "amount",
        "payment_purpose": "details",
        "counterparty_name": "operation",
        "document_number": "page",
    },
    "Kaspi Pay": {
        "operation_date": "Дата операции",
        "debit_amount": "Дебет",
        "credit_amount": "Кредит",
        "payment_code_knp": "КНП",
        "payment_purpose": "Назначение платежа",
        "counterparty_name": "Наименование получателя",
        "document_number": "Номер документа",
    },
    "Halyk Business": {
        "operation_date": "Дата",
        "debit_amount": "Дебет",
        "credit_amount": "Кредит",
        "payment_code_knp": "КНП",
        "payment_purpose": "Детали платежа",
        "counterparty_name": "Контрагент (имя)",
        "counterparty_iin_bin": "Контрагент ИИН/БИН",
        "document_number": "Номер документа",
    },
    "Halyk Individual": {
        "operation_date": "Дата проведения операции",
        "credit_amount": "Приход в валюте счета",
        "debit_amount": "Расход в валюте счета",
        "payment_code_knp": "КНП",
        "payment_purpose": "Описание операции",
        "counterparty_name": "Описание операции",
    },
    "BCC Bank": {
        "operation_date": "Күні / Дата",
        "debit_amount": "Дебет / Дебет",
        "credit_amount": "Кредит / Кредит",
        "payment_code_knp": "ТМК /КНП",
        "payment_purpose": "Төлемнің мақсаты / Назначение платежа",
        "counterparty_name": "Корреспондент / Корреспондент",
    },
    "Eurasian Bank": {
        "operation_date": "Дата проводки",
        "debit_amount": "Дебет",
        "credit_amount": "Кредит",
        "payment_code_knp": "КНП",
        "payment_purpose": "Назначение платежа",
        "counterparty_name": "Наименование Бенефициара/Отправителя",
        "document_number": "Номер документа клиента",
    },
    "Forte Bank": {
        "operation_date": "Дата",
        "debit_amount": "Дебет",
        "credit_amount": "Кредит",
        "payment_code_knp": "КНП",
        "payment_purpose": "Назначение платежа",
        "counterparty_name": "Жіберуші/Отправитель",
        "document_number": "Номер документа",
    },
}


class DatabaseConnection:
    """PostgreSQL database connection handler"""

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        sslmode: Optional[str] = None,
    ):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.sslmode = sslmode
        self.connection = None

    def connect(self):
        """Establish database connection"""
        try:
            connect_kwargs = {
                "host": self.host,
                "port": self.port,
                "database": self.database,
                "user": self.user,
                "password": self.password,
                "connect_timeout": 10,
            }
            # Some managed PostgreSQL deployments require explicit SSL mode.
            if self.sslmode:
                connect_kwargs["sslmode"] = self.sslmode

            self.connection = psycopg2.connect(**connect_kwargs)
            print(f"✓ Connected to {self.database}@{self.host}:{self.port}")
        except psycopg2.Error as e:
            print(f"✗ Connection failed: {e}")
            raise

    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            print("Disconnected from database")

    # Whitelist for safe table/identifier usage (prevents SQL injection)
    ALLOWED_TABLES = frozenset({
        'projects', 'project_statements',
        'clients', 'accounts', 'statements', 'statement_headers',
        'transactions', 'transactions_ip_flags', 'income_summaries',
        'ip_income_monthly', 'statement_footers', 'statement_metadata',
        'counterparties'
    })

    def execute_query(self, query: str, params: tuple = None) -> List[Dict]:
        """Execute SELECT query and return results"""
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, params)
            columns = [desc[0] for desc in cursor.description]
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            return results
        finally:
            cursor.close()

    def safe_count_table(self, table_name: str) -> int:
        """Safely get row count for a whitelisted table (SQL injection safe)"""
        if table_name not in self.ALLOWED_TABLES:
            raise ValueError(f"Table name not in whitelist: {table_name}")
        cursor = self.connection.cursor()
        try:
            q = sql.SQL("SELECT COUNT(*) as count FROM {}").format(sql.Identifier(table_name))
            cursor.execute(q)
            row = cursor.fetchone()
            return row[0] if row else 0
        finally:
            cursor.close()

    def safe_truncate_table(self, table_name: str) -> None:
        """Safely truncate a whitelisted table (SQL injection safe)"""
        if table_name not in self.ALLOWED_TABLES:
            raise ValueError(f"Table name not in whitelist: {table_name}")
        cursor = self.connection.cursor()
        try:
            q = sql.SQL("TRUNCATE TABLE {} CASCADE").format(sql.Identifier(table_name))
            cursor.execute(q)
        finally:
            cursor.close()

    def execute_insert(self, query: str, params: tuple = None) -> str:
        """Execute INSERT query and return inserted ID"""
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, params)
            self.connection.commit()
            # Try to fetch the returning ID
            result = cursor.fetchone()
            return result[0] if result else None
        except psycopg2.Error as e:
            self.connection.rollback()
            print(f"Insert error: {e}")
            raise
        finally:
            cursor.close()

    def execute_many(self, query: str, params_list: List[tuple]) -> int:
        """Execute multiple INSERT queries"""
        cursor = self.connection.cursor()
        try:
            cursor.executemany(query, params_list)
            self.connection.commit()
            return cursor.rowcount
        except psycopg2.Error as e:
            self.connection.rollback()
            print(f"Batch insert error: {e}")
            raise
        finally:
            cursor.close()

    def execute_command(self, query: str, params: tuple = None) -> None:
        """Execute command/query without returning rows"""
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, params)
            self.connection.commit()
        except psycopg2.Error as e:
            self.connection.rollback()
            print(f"Command error: {e}")
            raise
        finally:
            cursor.close()

    def ensure_project_schema(self) -> None:
        """
        Runtime-safe schema adjustments for project-based workflow.
        Uses IF NOT EXISTS to stay backward-compatible on existing DBs.
        """
        ddl_statements = [
            """
            ALTER TABLE statements
            ADD COLUMN IF NOT EXISTS first_operation_date DATE
            """,
            """
            ALTER TABLE statements
            ADD COLUMN IF NOT EXISTS last_operation_date DATE
            """,
            """
            ALTER TABLE statements
            ADD COLUMN IF NOT EXISTS calc_start_date DATE
            """,
            """
            ALTER TABLE statements
            ADD COLUMN IF NOT EXISTS calc_end_date DATE
            """,
            """
            ALTER TABLE statements
            ADD COLUMN IF NOT EXISTS uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            """,
            """
            CREATE TABLE IF NOT EXISTS projects (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                name VARCHAR(255) NOT NULL,
                status VARCHAR(50) NOT NULL DEFAULT 'draft',
                created_by VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS project_statements (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
                statement_id UUID REFERENCES statements(id) ON DELETE CASCADE,
                upload_order SMALLINT NOT NULL,
                source_filename VARCHAR(500),
                processing_status VARCHAR(50) NOT NULL,
                processing_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            ALTER TABLE project_statements
            ALTER COLUMN statement_id DROP NOT NULL
            """,
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_project_statements_statement
            ON project_statements(statement_id)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_project_statements_project
            ON project_statements(project_id)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_project_statements_upload_order
            ON project_statements(project_id, upload_order)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_statements_uploaded_at
            ON statements(uploaded_at)
            """,
            """
            UPDATE statements
            SET uploaded_at = COALESCE(uploaded_at, created_at, CURRENT_TIMESTAMP)
            WHERE uploaded_at IS NULL
            """,
        ]
        for ddl in ddl_statements:
            self.execute_command(ddl)


class StatementRepository:
    """Repository for statement-related database operations"""

    def __init__(self, db: DatabaseConnection):
        self.db = db

    def get_or_create_client(self, iin_bin: str, full_name: str, client_type: str = "IP") -> str:
        """Get existing client or create new one. Returns client ID."""
        # Convert numpy types to Python types
        iin_bin = str(iin_bin) if iin_bin else None
        full_name = str(full_name).strip() if full_name else ""
        
        if not iin_bin:
            raise ValueError("IIN/BIN cannot be empty")
        if not full_name:
            # DB column clients.full_name is NOT NULL.
            # Use deterministic fallback when parser can't extract the name.
            full_name = f"Клиент {iin_bin}"
        
        # Try to get existing
        query = "SELECT id FROM clients WHERE iin_bin = %s"
        results = self.db.execute_query(query, (iin_bin,))
        if results:
            return results[0]["id"]

        # Create new
        query = """
        INSERT INTO clients (iin_bin, full_name, client_type)
        VALUES (%s, %s, %s)
        RETURNING id
        """
        client_id = self.db.execute_insert(query, (iin_bin, full_name, client_type))
        return client_id

    def get_or_create_account(self, client_id: str, account_number: str, bank: str, currency: str = "KZT") -> str:
        """Get existing account or create new one. Returns account ID."""
        # Convert numpy types to Python types
        account_number = str(account_number) if account_number else None
        bank = str(bank) if bank else None
        
        if not account_number:
            raise ValueError("Account number cannot be empty")
        
        # Try to get existing
        query = "SELECT id FROM accounts WHERE account_number = %s"
        results = self.db.execute_query(query, (account_number,))
        if results:
            return results[0]["id"]

        # Create new
        query = """
        INSERT INTO accounts (client_id, account_number, bank, currency)
        VALUES (%s, %s, %s, %s)
        RETURNING id
        """
        account_id = self.db.execute_insert(query, (client_id, account_number, bank, currency))
        return account_id

    def create_statement(
        self,
        account_id: str,
        bank: str,
        pdf_name: str,
        period_from: date,
        period_to: date,
        statement_generation_date: date,
        first_operation_date: Optional[date] = None,
        last_operation_date: Optional[date] = None,
        calc_start_date: Optional[date] = None,
        calc_end_date: Optional[date] = None,
    ) -> str:
        """Create new statement. Returns statement ID."""
        query = """
        INSERT INTO statements (
            account_id, bank, pdf_name, period_from, period_to, statement_generation_date,
            first_operation_date, last_operation_date, calc_start_date, calc_end_date, uploaded_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        RETURNING id
        """
        statement_id = self.db.execute_insert(
            query,
            (
                account_id, bank, pdf_name, period_from, period_to, statement_generation_date,
                first_operation_date, last_operation_date, calc_start_date, calc_end_date
            )
        )
        return statement_id

    def create_statement_header(self, statement_id: str, header_data: Dict) -> str:
        """Create statement header record"""
        query = """
        INSERT INTO statement_headers (
            statement_id, account_number, currency, account_holder_name, iin_bin,
            period_from, period_to, opening_balance, opening_balance_date,
            closing_balance, closing_balance_date, debit_turnover, credit_turnover,
            raw_header_text
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """
        return self.db.execute_insert(query, (
            statement_id,
            header_data.get("account_number"),
            header_data.get("currency", "KZT"),
            header_data.get("account_holder_name"),
            header_data.get("iin_bin"),
            self._safe_date(header_data.get("period_from")),
            self._safe_date(header_data.get("period_to")),
            self._safe_float(header_data.get("opening_balance")),
            self._safe_date(header_data.get("opening_balance_date")),
            self._safe_float(header_data.get("closing_balance")),
            self._safe_date(header_data.get("closing_balance_date")),
            self._safe_float(header_data.get("debit_turnover")),
            self._safe_float(header_data.get("credit_turnover")),
            header_data.get("raw_header_text"),
        ))

    def insert_transactions(self, statement_id: str, transactions_df: pd.DataFrame, bank: str) -> int:
        """Insert transactions for a statement"""
        mapping = COLUMN_MAPPINGS.get(bank, {})

        def get_value(row, unified_col: str):
            """Get value from row using mapping"""
            mapped_col = mapping.get(unified_col)
            if mapped_col and mapped_col in row:
                val = row[mapped_col]
                # Handle NaN and None
                if pd.isna(val):
                    return None
                return val
            return None

        transaction_ids = []
        for _, row in transactions_df.iterrows():
            operation_date_str = get_value(row, "operation_date")
            if not operation_date_str:
                continue

            # Parse operation date
            try:
                if isinstance(operation_date_str, pd.Timestamp):
                    operation_date = operation_date_str.date()
                elif isinstance(operation_date_str, str):
                    # Try multiple date formats
                    for fmt in ["%d.%m.%Y", "%Y-%m-%d", "%d.%m.%y"]:
                        try:
                            operation_date = datetime.strptime(operation_date_str.split()[0], fmt).date()
                            break
                        except ValueError:
                            continue
                    else:
                        operation_date = None
                else:
                    operation_date = operation_date_str
            except Exception:
                continue

            if not operation_date:
                continue

            query = """
            INSERT INTO transactions (
                statement_id, operation_date, credit_amount, debit_amount,
                payment_code_knp, payment_purpose, counterparty_name,
                counterparty_iin_bin, counterparty_account, counterparty_bank_bic,
                document_number, raw_operation_text
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """

            tx_id = self.db.execute_insert(query, (
                statement_id,
                operation_date,
                self._safe_float(get_value(row, "credit_amount")),
                self._safe_float(get_value(row, "debit_amount")),
                get_value(row, "payment_code_knp"),
                get_value(row, "payment_purpose"),
                get_value(row, "counterparty_name"),
                get_value(row, "counterparty_iin_bin"),
                get_value(row, "counterparty_account"),
                get_value(row, "counterparty_bank_bic"),
                get_value(row, "document_number"),
                str(dict(row)) if isinstance(row, pd.Series) else None,
            ))
            transaction_ids.append(tx_id)

        return len(transaction_ids)

    def insert_ip_flags(self, statement_id: str, tx_ip_df: pd.DataFrame) -> int:
        """Insert IP income flags for transactions"""
        count = 0
        for _, row in tx_ip_df.iterrows():
            # Find transaction by document number (unique identifier)
            doc_number = row.get("document_number") or row.get("№ док")
            
            if not doc_number:
                continue

            query = """
            SELECT id FROM transactions
            WHERE statement_id = %s AND document_number = %s
            LIMIT 1
            """
            results = self.db.execute_query(query, (statement_id, str(doc_number)))

            if not results:
                continue

            transaction_id = results[0]["id"]

            query = """
            INSERT INTO transactions_ip_flags (
                transaction_id, statement_id, knp_normalized,
                is_non_business_by_knp, is_non_business_by_keywords,
                is_non_business, is_business_income, ip_credit_amount
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """

            self.db.execute_insert(query, (
                transaction_id,
                statement_id,
                row.get("ip_knp_norm"),
                bool(row.get("ip_is_non_business_by_knp", False)),
                bool(row.get("ip_is_non_business_by_keywords", False)),
                bool(row.get("ip_is_non_business", False)),
                bool(row.get("ip_is_business_income", False)),
                self._safe_float(row.get("ip_credit_amount")),
            ))
            count += 1

        return count

    def insert_income_summary(self, statement_id: str, summary_data: Dict) -> str:
        """Insert income summary"""
        query = """
        INSERT INTO income_summaries (
            statement_id, total_income, total_income_adjusted,
            total_sum, max_transaction, min_transaction, mean_transaction,
            transactions_used, formula, calculation_date
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """

        return self.db.execute_insert(query, (
            statement_id,
            self._safe_float(summary_data.get("total_income")),
            self._safe_float(summary_data.get("total_income_adjusted")),
            self._safe_float(summary_data.get("total_sum")),
            self._safe_float(summary_data.get("max_transaction")),
            self._safe_float(summary_data.get("min_transaction")),
            self._safe_float(summary_data.get("mean_transaction")),
            int(summary_data.get("transactions_used", 0)),
            summary_data.get("formula"),
            datetime.now(),
        ))

    def insert_monthly_income(self, statement_id: str, monthly_income_df: pd.DataFrame) -> int:
        """Insert monthly income records"""
        count = 0
        for _, row in monthly_income_df.iterrows():
            month_str = row.get("month")
            if not month_str:
                continue

            # Parse month
            try:
                if isinstance(month_str, str):
                    month = datetime.strptime(month_str, "%Y-%m").date()
                else:
                    month = month_str
            except Exception:
                continue

            query = """
            INSERT INTO ip_income_monthly (statement_id, month, business_income, transaction_count)
            VALUES (%s, %s, %s, %s)
            RETURNING id
            """

            self.db.execute_insert(query, (
                statement_id,
                month,
                self._safe_float(row.get("business_income")),
                int(row.get("transaction_count", 0)),
            ))
            count += 1

        return count

    def insert_statement_footer(self, statement_id: str, footer_data: Dict) -> str:
        """Insert statement footer"""
        query = """
        INSERT INTO statement_footers (
            statement_id, total_debit, total_credit, final_balance, final_balance_date, raw_footer_text
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id
        """

        return self.db.execute_insert(query, (
            statement_id,
            self._safe_float(footer_data.get("total_debit_footer")),
            self._safe_float(footer_data.get("total_credit_footer")),
            self._safe_float(footer_data.get("final_balance")),
            self._safe_date(footer_data.get("final_balance_date")),
            footer_data.get("raw_footer_row"),
        ))

    def insert_metadata(self, statement_id: str, metadata: Dict) -> str:
        """Insert statement metadata"""
        query = """
        INSERT INTO statement_metadata (
            statement_id, validation_flags, validation_score, processor,
            opening_balance, closing_balance, balance_matches, debug_info
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """

        debug_info = metadata.get("debug_info", {})
        if not isinstance(debug_info, str):
            debug_info = json.dumps(debug_info)

        return self.db.execute_insert(query, (
            statement_id,
            metadata.get("flags", ""),
            self._safe_float(metadata.get("score")),
            metadata.get("processor"),
            self._safe_float(metadata.get("opening_balance")),
            self._safe_float(metadata.get("closing_balance")),
            metadata.get("balance_matches", False),
            debug_info,
        ))

    @staticmethod
    def _safe_float(value) -> Optional[float]:
        """Safely convert value to float, handling numpy types"""
        if value is None:
            return None
        if isinstance(value, bool):  # bool before int check since bool is subclass of int
            return None
        try:
            # Convert to float, which handles numpy types
            f = float(value)
            if pd.isna(f):
                return None
            return f
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _safe_date(date_value) -> Optional[date]:
        """Safely convert value to date, handling various formats"""
        if date_value is None:
            return None
        
        try:
            if isinstance(date_value, date):
                return date_value
            elif isinstance(date_value, pd.Timestamp):
                return date_value.date()
            elif isinstance(date_value, str):
                # Try multiple date formats
                for fmt in ["%d.%m.%Y", "%Y-%m-%d", "%d.%m.%y"]:
                    try:
                        return datetime.strptime(date_value.split()[0], fmt).date()
                    except ValueError:
                        continue
            return None
        except Exception:
            return None


# Helper function for batch import
def import_statement_to_db(
    db: DatabaseConnection,
    statement_data: Dict[str, Any],
    bank: str,
) -> str:
    """
    Import a complete statement to database

    Args:
        db: Database connection
        statement_data: Dictionary with statement components:
            {
                'header_df': DataFrame,
                'tx_df': DataFrame,
                'footer_df': DataFrame,
                'meta_df': DataFrame,
                'tx_ip_df': DataFrame (enriched),
                'monthly_income_df': DataFrame,
                'income_summary': dict,
                'client_iin': str,
                'client_name': str,
                'pdf_name': str,
                'account_number': str,
            }
        bank: Bank name

    Returns:
        Statement ID
    """
    repo = StatementRepository(db)

    # 1. Get or create client
    iin = statement_data.get("client_iin")
    name = statement_data.get("client_name")
    client_id = repo.get_or_create_client(iin, name)

    # 2. Get or create account
    account_number = statement_data.get("account_number")
    if not account_number:
        header_df = statement_data.get("header_df")
        if header_df is not None and len(header_df) > 0:
            header_row = header_df.iloc[0]
            account_number = (
                header_row.get("Счет")
                or header_row.get("Счёт")
                or header_row.get("Номер счета")
                or header_row.get("account_number")
                or header_row.get("IBAN")
                or header_row.get("iban")
                or header_row.get("ИИК")
            )
    account_id = repo.get_or_create_account(client_id, account_number, bank)

    # 3. Create statement
    header_df = statement_data.get("header_df")
    if header_df is not None and len(header_df) > 0:
        period_from = header_df.iloc[0].get("period_from") if "period_from" in header_df.columns else None
        period_to = header_df.iloc[0].get("period_to") if "period_to" in header_df.columns else None
    else:
        period_from = period_to = None

    tx_df = statement_data.get("tx_df")

    def _safe_ts_date(v) -> Optional[date]:
        if v is None:
            return None
        parsed = pd.to_datetime(v, errors="coerce", dayfirst=True)
        if pd.isna(parsed):
            return None
        return parsed.date()

    statement_date = None
    if header_df is not None and len(header_df) > 0:
        header_row = header_df.iloc[0]
        statement_date = (
            _safe_ts_date(header_row.get("Дата получения выписки"))
            or _safe_ts_date(header_row.get("statement_generation_date"))
            or _safe_ts_date(header_row.get("period_to"))
            or _safe_ts_date(header_row.get("Период (конец)"))
        )

    if statement_date is None:
        statement_date = period_to or date.today()

    first_operation_date = None
    last_operation_date = None
    if tx_df is not None and len(tx_df) > 0:
        tx_dates = None
        for candidate in ["txn_date", "Дата", "Дата операции", "Дата проводки", "operation_date"]:
            if candidate in tx_df.columns:
                tx_dates = pd.to_datetime(tx_df[candidate], errors="coerce", dayfirst=True).dropna()
                if not tx_dates.empty:
                    break
        if tx_dates is not None and not tx_dates.empty:
            first_operation_date = tx_dates.min().date()
            last_operation_date = tx_dates.max().date()

    # Ensure date fields are always filled
    if first_operation_date is None:
        first_operation_date = statement_date
    if last_operation_date is None:
        last_operation_date = statement_date

    # Calculation window (test mode): end = statement date.
    calc_end_date = statement_date
    calc_end_month_start = calc_end_date.replace(day=1)
    start_month_index = calc_end_month_start.year * 12 + calc_end_month_start.month - 1 - 11
    start_year = start_month_index // 12
    start_month = start_month_index % 12 + 1
    calc_start_date = date(start_year, start_month, 1)

    pdf_name = statement_data.get("pdf_name")
    statement_id = repo.create_statement(
        account_id,
        bank,
        pdf_name,
        period_from,
        period_to,
        statement_date,
        first_operation_date=first_operation_date,
        last_operation_date=last_operation_date,
        calc_start_date=calc_start_date,
        calc_end_date=calc_end_date,
    )

    # 4. Insert header
    if header_df is not None and len(header_df) > 0:
        header_dict = header_df.iloc[0].to_dict()
        repo.create_statement_header(statement_id, header_dict)

    # 5. Insert transactions
    tx_df = statement_data.get("tx_df")
    if tx_df is not None and len(tx_df) > 0:
        repo.insert_transactions(statement_id, tx_df, bank)

    # 6. Insert IP flags
    tx_ip_df = statement_data.get("tx_ip_df")
    if tx_ip_df is not None and len(tx_ip_df) > 0:
        repo.insert_ip_flags(statement_id, tx_ip_df)

    # 7. Insert income summary
    income_summary = statement_data.get("income_summary")
    if income_summary:
        repo.insert_income_summary(statement_id, income_summary)

    # 8. Insert monthly income
    monthly_df = statement_data.get("monthly_income_df")
    if monthly_df is not None and len(monthly_df) > 0:
        repo.insert_monthly_income(statement_id, monthly_df)

    # 9. Insert footer
    footer_df = statement_data.get("footer_df")
    if footer_df is not None and len(footer_df) > 0:
        footer_dict = footer_df.iloc[0].to_dict()
        repo.insert_statement_footer(statement_id, footer_dict)

    # 10. Insert metadata
    meta_df = statement_data.get("meta_df")
    if meta_df is not None and len(meta_df) > 0:
        meta_dict = meta_df.iloc[0].to_dict()
        repo.insert_metadata(statement_id, meta_dict)

    return statement_id
