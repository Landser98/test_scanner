#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bank detection module for automatically detecting bank type from PDF.
"""

from __future__ import annotations

from typing import Optional
import tempfile
from pathlib import Path

# --- ensure project root on sys.path ---
import sys
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.core.service import BANK_DISPATCH


def detect_bank_from_pdf(pdf_bytes: bytes, pdf_name: str) -> Optional[str]:
    """
    Try to detect bank type by attempting to parse with different parsers.
    Returns bank_key if successful, None otherwise.
    """
    # Order of attempts (most common first)
    bank_order = [
        "kaspi_gold",
        "kaspi_pay",
        "halyk_business",
        "halyk_individual",
        "bcc_bank",
        "alatau_city_bank",
        "eurasian_bank",
        "forte_bank",
        "freedom_bank",
    ]
    
    for bank_key in bank_order:
        try:
            parser = BANK_DISPATCH.get(bank_key)
            if parser is None:
                continue
            
            # Try to parse with this bank's parser
            statement = parser(pdf_name, pdf_bytes)
            
            # If parsing succeeded and we got a statement with transactions, it's likely this bank
            if statement and hasattr(statement, 'tx_df') and not statement.tx_df.empty:
                return bank_key
        except Exception:
            # Try next bank
            continue
    
    return None

