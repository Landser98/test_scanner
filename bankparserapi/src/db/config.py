"""
Database Configuration

Configure your PostgreSQL connection settings here.
"""

import os
from typing import Dict
from src.utils.vault_loader import load_vault_config_once

# Load Vault-backed environment once before reading DB settings.
load_vault_config_once()

# Read from environment variables or use defaults
DB_CONFIG: Dict[str, any] = {
    'host': os.getenv('DB_HOST', '127.0.0.1'),
    'port': int(os.getenv('DB_PORT', '5432')),
    'database': os.getenv('DB_NAME', 'bank_statements'),
    'user': os.getenv('DB_USER', 'bank_user'),
    'password': os.getenv('DB_PASSWORD', 'secure_password'),
    'sslmode': os.getenv('DB_SSLMODE', None),
}

# Development mode flag
DEVELOPMENT = os.getenv('ENVIRONMENT', 'development').lower() == 'development'

# Logging configuration
LOGGING = {
    'level': 'DEBUG' if DEVELOPMENT else 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
}
