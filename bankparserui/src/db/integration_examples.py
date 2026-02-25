"""
Integration examples for importing parsed statements to the unified database

This module shows how to integrate each bank's parser with the unified database.
"""

from pathlib import Path
import pandas as pd
from datetime import datetime
from src.db.database import DatabaseConnection, import_statement_to_db

# Database configuration
# SECURITY: Use environment variables instead of hardcoded credentials
import os
DB_CONFIG = {
    'host': os.environ.get('DB_HOST', 'localhost'),
    'port': int(os.environ.get('DB_PORT', '5432')),
    'database': os.environ.get('DB_NAME', 'bank_statements'),
    'user': os.environ.get('DB_USER', 'bank_user'),
    'password': os.environ.get('DB_PASSWORD', ''),  # Must be set via environment variable
}


# ============================================================================
# Example 1: Import Alatau City Bank Statement
# ============================================================================

def import_alatau_statement(csv_dir: str):
    """
    Import Alatau City Bank statement from CSV files
    
    Expected CSV files:
    - *_header.csv
    - *_tx.csv
    - *_footer.csv
    - *_meta.csv
    - *_tx_ip.csv
    - *_ip_income_monthly.csv
    - *_income_summary.csv
    """
    csv_path = Path(csv_dir)
    
    # Find CSV files (assumes single statement per directory)
    header_files = list(csv_path.glob('*_header.csv'))
    if not header_files:
        print("No header CSV found")
        return None
    
    stem = header_files[0].stem.replace('_header', '')
    
    # Read CSV files
    header_df = pd.read_csv(csv_path / f'{stem}_header.csv')
    tx_df = pd.read_csv(csv_path / f'{stem}_tx.csv')
    footer_df = pd.read_csv(csv_path / f'{stem}_footer.csv')
    meta_df = pd.read_csv(csv_path / f'{stem}_meta.csv')
    tx_ip_df = pd.read_csv(csv_path / f'{stem}_tx_ip.csv')
    monthly_df = pd.read_csv(csv_path / f'{stem}_ip_income_monthly.csv')
    income_summary_df = pd.read_csv(csv_path / f'{stem}_income_summary.csv')
    
    # Prepare statement data
    statement_data = {
        'header_df': header_df,
        'tx_df': tx_df,
        'footer_df': footer_df,
        'meta_df': meta_df,
        'tx_ip_df': tx_ip_df,
        'monthly_income_df': monthly_df,
        'income_summary': income_summary_df.iloc[0].to_dict() if len(income_summary_df) > 0 else {},
        'client_iin': header_df.iloc[0]['iin_bin'] if 'iin_bin' in header_df.columns else None,
        'client_name': header_df.iloc[0]['client'] if 'client' in header_df.columns else None,
        'account_number': header_df.iloc[0]['account'] if 'account' in header_df.columns else None,
        'pdf_name': stem + '.pdf',
    }
    
    # Connect and import
    db = DatabaseConnection(**DB_CONFIG)
    db.connect()
    try:
        statement_id = import_statement_to_db(db, statement_data, 'Alatau City Bank')
        print(f"✓ Alatau City Bank statement imported: {statement_id}")
        return statement_id
    finally:
        db.disconnect()


# ============================================================================
# Example 2: Import Kaspi Gold Statement
# ============================================================================

def import_kaspi_gold_statement(csv_dir: str):
    """
    Import Kaspi Gold statement from CSV files
    
    Expected CSV files:
    - *_header.csv
    - *_tx.csv
    - *_meta.csv
    - *_tx_ip.csv
    - *_ip_income_monthly.csv
    - *_income_summary.csv
    """
    csv_path = Path(csv_dir)
    
    # Find CSV files
    header_files = list(csv_path.glob('*_header.csv'))
    if not header_files:
        print("No header CSV found")
        return None
    
    stem = header_files[0].stem.replace('_header', '')
    
    # Read CSV files
    header_df = pd.read_csv(csv_path / f'{stem}_header.csv')
    tx_df = pd.read_csv(csv_path / f'{stem}_tx.csv')
    meta_df = pd.read_csv(csv_path / f'{stem}_meta.csv')
    tx_ip_df = pd.read_csv(csv_path / f'{stem}_tx_ip.csv')
    monthly_df = pd.read_csv(csv_path / f'{stem}_ip_income_monthly.csv')
    income_summary_df = pd.read_csv(csv_path / f'{stem}_income_summary.csv')
    
    # Footer may not exist for Kaspi Gold
    footer_df = None
    footer_files = list(csv_path.glob(f'{stem}_footer.csv'))
    if footer_files:
        footer_df = pd.read_csv(footer_files[0])
    
    # Prepare statement data
    statement_data = {
        'header_df': header_df,
        'tx_df': tx_df,
        'footer_df': footer_df,
        'meta_df': meta_df,
        'tx_ip_df': tx_ip_df,
        'monthly_income_df': monthly_df,
        'income_summary': income_summary_df.iloc[0].to_dict() if len(income_summary_df) > 0 else {},
        'client_iin': meta_df.iloc[0].get('iin_bin') if 'iin_bin' in meta_df.columns else None,
        'client_name': meta_df.iloc[0].get('client_name') if 'client_name' in meta_df.columns else None,
        'account_number': header_df.iloc[0].get('account') if 'account' in header_df.columns else None,
        'pdf_name': stem + '.pdf',
    }
    
    # Connect and import
    db = DatabaseConnection(**DB_CONFIG)
    db.connect()
    try:
        statement_id = import_statement_to_db(db, statement_data, 'Kaspi Gold')
        print(f"✓ Kaspi Gold statement imported: {statement_id}")
        return statement_id
    finally:
        db.disconnect()


# ============================================================================
# Example 3: Import Kaspi Pay Statement
# ============================================================================

def import_kaspi_pay_statement(csv_dir: str):
    """
    Import Kaspi Pay statement from CSV files
    """
    csv_path = Path(csv_dir)
    
    header_files = list(csv_path.glob('*_header.csv'))
    if not header_files:
        return None
    
    stem = header_files[0].stem.replace('_header', '')
    
    header_df = pd.read_csv(csv_path / f'{stem}_header.csv')
    tx_df = pd.read_csv(csv_path / f'{stem}_tx.csv')
    tx_ip_df = pd.read_csv(csv_path / f'{stem}_tx_ip.csv')
    monthly_df = pd.read_csv(csv_path / f'{stem}_ip_income_monthly.csv')
    income_summary_df = pd.read_csv(csv_path / f'{stem}_income_summary.csv')
    meta_df = pd.read_csv(csv_path / f'{stem}_meta.csv') if (csv_path / f'{stem}_meta.csv').exists() else None
    footer_df = pd.read_csv(csv_path / f'{stem}_footer.csv') if (csv_path / f'{stem}_footer.csv').exists() else None
    
    statement_data = {
        'header_df': header_df,
        'tx_df': tx_df,
        'footer_df': footer_df,
        'meta_df': meta_df,
        'tx_ip_df': tx_ip_df,
        'monthly_income_df': monthly_df,
        'income_summary': income_summary_df.iloc[0].to_dict() if len(income_summary_df) > 0 else {},
        'client_iin': header_df.iloc[0].get('iin_bin'),
        'client_name': header_df.iloc[0].get('client_name'),
        'account_number': header_df.iloc[0].get('account'),
        'pdf_name': stem + '.pdf',
    }
    
    db = DatabaseConnection(**DB_CONFIG)
    db.connect()
    try:
        statement_id = import_statement_to_db(db, statement_data, 'Kaspi Pay')
        print(f"✓ Kaspi Pay statement imported: {statement_id}")
        return statement_id
    finally:
        db.disconnect()


# ============================================================================
# Example 4: Import Halyk Bank Statement (Business or Individual)
# ============================================================================

def import_halyk_statement(csv_dir: str, account_type: str = 'business'):
    """
    Import Halyk Bank statement (Business or Individual)
    
    Args:
        csv_dir: Directory with CSV files
        account_type: 'business' or 'individual'
    """
    csv_path = Path(csv_dir)
    
    header_files = list(csv_path.glob('*_header.csv'))
    if not header_files:
        return None
    
    stem = header_files[0].stem.replace('_header', '')
    bank_name = 'Halyk Business' if account_type == 'business' else 'Halyk Individual'
    
    header_df = pd.read_csv(csv_path / f'{stem}_header.csv')
    tx_df = pd.read_csv(csv_path / f'{stem}_tx.csv')
    tx_ip_df = pd.read_csv(csv_path / f'{stem}_tx_ip.csv')
    monthly_df = pd.read_csv(csv_path / f'{stem}_ip_income_monthly.csv')
    income_summary_df = pd.read_csv(csv_path / f'{stem}_income_summary.csv')
    meta_df = pd.read_csv(csv_path / f'{stem}_meta.csv') if (csv_path / f'{stem}_meta.csv').exists() else None
    footer_df = pd.read_csv(csv_path / f'{stem}_footer.csv') if (csv_path / f'{stem}_footer.csv').exists() else None
    
    statement_data = {
        'header_df': header_df,
        'tx_df': tx_df,
        'footer_df': footer_df,
        'meta_df': meta_df,
        'tx_ip_df': tx_ip_df,
        'monthly_income_df': monthly_df,
        'income_summary': income_summary_df.iloc[0].to_dict() if len(income_summary_df) > 0 else {},
        'client_iin': header_df.iloc[0].get('iin_bin'),
        'client_name': header_df.iloc[0].get('account_holder_name'),
        'account_number': header_df.iloc[0].get('account_number'),
        'pdf_name': stem + '.pdf',
    }
    
    db = DatabaseConnection(**DB_CONFIG)
    db.connect()
    try:
        statement_id = import_statement_to_db(db, statement_data, bank_name)
        print(f"✓ {bank_name} statement imported: {statement_id}")
        return statement_id
    finally:
        db.disconnect()


# ============================================================================
# Example 5: Batch Import from Multiple PDFs
# ============================================================================

def batch_import_statements(csv_base_dir: str):
    """
    Batch import all statements from a directory structure
    
    Expected structure:
    csv_base_dir/
    ├── alatau_city_bank_out/
    │   ├── *_header.csv
    │   ├── *_tx.csv
    │   └── ...
    ├── kaspi_gold_out/
    │   ├── *_header.csv
    │   └── ...
    └── ...
    """
    base_path = Path(csv_base_dir)
    results = []
    
    # Bank directories and their import functions
    bank_dirs = {
        'alatau_city_bank_out': ('Alatau City Bank', import_alatau_statement),
        'kaspi_gold_out': ('Kaspi Gold', import_kaspi_gold_statement),
        'kaspi_pay_out': ('Kaspi Pay', import_kaspi_pay_statement),
        'halyk_business_out': ('Halyk Business', import_halyk_statement),
        'halyk_individual_out': ('Halyk Individual', import_halyk_statement),
        'bcc_out': ('BCC Bank', import_kaspi_pay_statement),  # BCC uses similar format
        'eurasian_bank_out': ('Eurasian Bank', import_kaspi_pay_statement),
        'forte_bank_out': ('Forte Bank', import_kaspi_pay_statement),
    }
    
    for dir_name, (bank_name, import_func) in bank_dirs.items():
        dir_path = base_path / dir_name
        if dir_path.exists():
            print(f"\n📂 Processing {bank_name}...")
            try:
                statement_id = import_func(str(dir_path))
                if statement_id:
                    results.append({
                        'bank': bank_name,
                        'statement_id': statement_id,
                        'status': 'success'
                    })
            except Exception as e:
                print(f"  ✗ Error: {e}")
                results.append({
                    'bank': bank_name,
                    'status': 'error',
                    'error': str(e)
                })
    
    return results


# ============================================================================
# Example: Query imported data
# ============================================================================

def query_all_statements():
    """Query all imported statements"""
    db = DatabaseConnection(**DB_CONFIG)
    db.connect()
    try:
        query = """
        SELECT 
            c.full_name,
            a.account_number,
            s.bank,
            s.period_from,
            s.period_to,
            COUNT(t.id) as transaction_count
        FROM statements s
        JOIN accounts a ON s.account_id = a.id
        JOIN clients c ON a.client_id = c.id
        LEFT JOIN transactions t ON s.id = t.statement_id
        GROUP BY c.full_name, a.account_number, s.bank, s.period_from, s.period_to
        ORDER BY s.period_to DESC
        """
        results = db.execute_query(query)
        return results
    finally:
        db.disconnect()


def query_ip_income_summary():
    """Query IP income summary for all statements"""
    db = DatabaseConnection(**DB_CONFIG)
    db.connect()
    try:
        query = """
        SELECT 
            c.full_name,
            a.account_number,
            s.bank,
            ism.total_income_adjusted,
            ism.mean_transaction,
            ism.transactions_used,
            s.period_from,
            s.period_to
        FROM income_summaries ism
        JOIN statements s ON ism.statement_id = s.id
        JOIN accounts a ON s.account_id = a.id
        JOIN clients c ON a.client_id = c.id
        ORDER BY s.period_to DESC
        """
        results = db.execute_query(query)
        return results
    finally:
        db.disconnect()


if __name__ == '__main__':
    # Example usage
    print("Bank Statement Database Integration Examples\n")
    
    # Batch import example
    # results = batch_import_statements('data')
    # print("\nImport results:")
    # for r in results:
    #     print(f"  {r['bank']}: {r['status']}")
    
    # Query examples
    print("\nQuerying imported statements...")
    statements = query_all_statements()
    print("\nAll Statements:")
    for s in statements:
        print(f"  {s['bank']:30} | {s['full_name']:30} | {s['period_from']} -> {s['period_to']}")
    
    print("\n\nIP Income Summary:")
    income = query_ip_income_summary()
    for row in income:
        print(f"  {row['full_name']:30} | Income: {row['total_income_adjusted']:>12.2f} | Avg: {row['mean_transaction']:>10.2f}")
