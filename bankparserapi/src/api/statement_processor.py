#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Statement processing module for API.
Handles parsing, validation, and analytics calculation.
"""

from __future__ import annotations

from datetime import date
from typing import List, Dict, Any, Tuple, Optional
import base64
import pandas as pd

# --- ensure project root on sys.path ---
import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.core.models import Statement
from src.core.service import parse_statement
from src.core.analysis import get_last_full_12m_window, compute_ip_income_for_statement
from src.api.bank_detector import detect_bank_from_pdf


class StatementProcessor:
    """Processes bank statements and calculates analytics"""
    
    # Status codes for statements
    STATUS_SUCCESS = 0
    STATUS_FAILURE = 1
    STATUS_SCANNED_COPY = 2
    STATUS_DATA_MISMATCH = 5
    
    def __init__(self):
        pass
    
    def parse_statement_base64(
        self,
        statement_id: str,
        statement_name: str,
        extension: str,
        base64_data: str,
        expected_iin: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Parse statement from base64 data.
        Returns dict with parsing result and status.
        """
        try:
            # Decode base64
            pdf_bytes = base64.b64decode(base64_data)
            
            # Detect bank type
            bank_key = detect_bank_from_pdf(pdf_bytes, statement_name)
            if bank_key is None:
                return {
                    "id": statement_id,
                    "name": statement_name,
                    "extension": extension,
                    "status": self.STATUS_FAILURE,
                    "message": "Не удалось определить тип банка. Статус – 1 LG",
                    "parsed_statement": None,
                    "error": "Bank detection failed"
                }
            
            # Parse statement
            try:
                statement = parse_statement(bank_key, statement_name, pdf_bytes)
            except Exception as e:
                return {
                    "id": statement_id,
                    "name": statement_name,
                    "extension": extension,
                    "status": self.STATUS_FAILURE,
                    "message": f"Ошибка при парсинге выписки. Статус – 1 LG: {str(e)}",
                    "parsed_statement": None,
                    "error": str(e)
                }
            
            # Validate IIN if provided
            if expected_iin and statement.iin_bin:
                if statement.iin_bin.strip() != expected_iin.strip():
                    return {
                        "id": statement_id,
                        "name": statement.account_holder_name or statement_name,
                        "extension": extension,
                        "status": self.STATUS_DATA_MISMATCH,
                        "message": f"Расхождение регистрационных данных. Статус – {self.STATUS_DATA_MISMATCH} LG",
                        "parsed_statement": statement,
                        "error": f"IIN mismatch: expected {expected_iin}, got {statement.iin_bin}"
                    }
            
            # Check for scanned copies (simplified check - if parsing failed or tx_df is empty)
            if statement.tx_df.empty:
                return {
                    "id": statement_id,
                    "name": statement.account_holder_name or statement_name,
                    "extension": extension,
                    "status": self.STATUS_SCANNED_COPY,
                    "message": f"Загружены сканированные копии документа. Статус – {self.STATUS_SCANNED_COPY} LG",
                    "parsed_statement": statement,
                    "error": "Empty transaction dataframe (possible scanned copy)"
                }
            
            # Success
            return {
                "id": statement_id,
                "name": statement.account_holder_name or statement_name,
                "extension": extension,
                "status": self.STATUS_SUCCESS,
                "message": "проект загружен",
                "parsed_statement": statement,
                "error": None
            }
            
        except Exception as e:
            return {
                "id": statement_id,
                "name": statement_name,
                "extension": extension,
                "status": self.STATUS_FAILURE,
                "message": f"Ошибка при обработке файла. Статус – 1 LG: {str(e)}",
                "parsed_statement": None,
                "error": str(e)
            }
    
    def calculate_analytics(
        self,
        statements: List[Statement],
        anchor_date: Optional[date] = None
    ) -> Dict[str, Any]:
        """
        Calculate analytics from parsed statements.
        Returns dict with iin, registration_date, average_income
        """
        if not statements:
            return {}
        
        if anchor_date is None:
            anchor_date = date.today()
        
        # Get 12-month window
        window_start, window_end = get_last_full_12m_window(anchor_date)
        
        # Extract IIN from first valid statement
        iin = None
        registration_date = None
        
        for stmt in statements:
            if stmt.iin_bin:
                iin = stmt.iin_bin.strip()
                break
        
        # For registration_date, we try to extract from statement dates
        # Use the earliest period_from as registration_date approximation
        earliest_date = None
        for stmt in statements:
            if stmt.period_from:
                if earliest_date is None or stmt.period_from < earliest_date:
                    earliest_date = stmt.period_from
        
        if earliest_date:
            registration_date = earliest_date.strftime("%d.%m.%Y")
        
        # Calculate average income
        # According to documentation examples, average_income appears to be total adjusted income
        # But logically it should be average monthly income over 12 months
        # We'll use total_income_adjusted / 12 to get monthly average
        total_income_adjusted = 0.0
        income_count = 0
        
        for stmt in statements:
            enriched, summary = compute_ip_income_for_statement(
                stmt,
                window_start,
                window_end
            )
            
            if summary and 'total_income_adjusted' in summary:
                # total_income_adjusted is the total adjusted income for 12 months
                # Convert to monthly average: total_income_adjusted / 12
                monthly_avg = summary['total_income_adjusted'] / 12.0
                total_income_adjusted += monthly_avg
                income_count += 1
        
        # Calculate average
        average_income = 0.0
        if income_count > 0:
            # Average across statements (already monthly averages)
            average_income = total_income_adjusted / income_count
        else:
            # Fallback: calculate from all credit transactions in window
            all_credits = []
            for stmt in statements:
                if stmt.tx_df.empty or 'txn_date' not in stmt.tx_df.columns:
                    continue
                
                df = stmt.tx_df.copy()
                df = df[df['txn_date'].notna()]
                
                # Filter by window
                mask = (
                    (df['txn_date'] >= pd.Timestamp(window_start)) &
                    (df['txn_date'] <= pd.Timestamp(window_end))
                )
                df_win = df.loc[mask]
                
                # Try to find credit column
                credit_cols = [col for col in df_win.columns if 'кредит' in col.lower() or 'credit' in col.lower() or col == 'amount']
                if credit_cols:
                    credit_col = credit_cols[0]
                    credits = df_win[credit_col].apply(lambda x: float(x) if pd.notna(x) and float(x) > 0 else 0.0)
                    all_credits.extend(credits[credits > 0].tolist())
            
            if all_credits:
                # Calculate monthly average from all credits (sum / 12 months)
                average_income = sum(all_credits) / 12.0
        
        # Build analytics dict
        analytics = {}
        if iin:
            analytics['iin'] = iin
        if registration_date:
            analytics['registration_date'] = registration_date
        if average_income > 0:
            analytics['average_income'] = round(average_income, 2)
        
        return analytics
    
    def check_iin_consistency(
        self,
        expected_iin: str,
        statements: List[Statement]
    ) -> Tuple[bool, List[str]]:
        """
        Check if all statements have matching IIN.
        Returns (is_consistent, list_of_mismatched_iins)
        """
        mismatched = []
        
        for stmt in statements:
            if stmt.iin_bin and stmt.iin_bin.strip() != expected_iin.strip():
                mismatched.append(stmt.iin_bin.strip())
        
        return len(mismatched) == 0, mismatched

