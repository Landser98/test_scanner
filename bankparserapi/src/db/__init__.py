"""
Unified Database Module for Bank Statements

This package provides database schema, connection management, and integration
utilities for storing and querying unified bank statements from multiple banks.

Main components:
- database.py: Core database operations and repository pattern
- integration_examples.py: Integration examples for each bank parser
- schema.sql: PostgreSQL database schema
- sample_data.sql: Sample data for testing
"""

from .database import (
    DatabaseConnection,
    StatementRepository,
    import_statement_to_db,
    BANK_NAMES,
    COLUMN_MAPPINGS,
)

__all__ = [
    'DatabaseConnection',
    'StatementRepository',
    'import_statement_to_db',
    'BANK_NAMES',
    'COLUMN_MAPPINGS',
]

__version__ = '1.0.0'
