#!/usr/bin/env python3
"""
Test script to import Alatau City Bank statement to PostgreSQL database
"""

import sys
from pathlib import Path
import pandas as pd

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.db.database import DatabaseConnection, import_statement_to_db
from src.db.config import DB_CONFIG

def import_alatau_statement():
    """Import Alatau City Bank statement from CSV files"""
    
    csv_dir = Path("/Users/david/Desktop/git/data/alatau_city_bank_out")
    
    # Find header file
    header_files = list(csv_dir.glob("*_header.csv"))
    if not header_files:
        print("❌ No header CSV found")
        return False
    
    stem = header_files[0].stem.replace("_header", "")
    print(f"📂 Processing: {stem}")
    
    # Read CSV files
    try:
        header_df = pd.read_csv(csv_dir / f"{stem}_header.csv")
        tx_df = pd.read_csv(csv_dir / f"{stem}_tx.csv")
        footer_df = pd.read_csv(csv_dir / f"{stem}_footer.csv")
        meta_df = pd.read_csv(csv_dir / f"{stem}_meta.csv")
        tx_ip_df = pd.read_csv(csv_dir / f"{stem}_tx_ip.csv")
        monthly_df = pd.read_csv(csv_dir / f"{stem}_ip_income_monthly.csv")
        income_summary_df = pd.read_csv(csv_dir / f"{stem}_income_summary.csv")
        
        print(f"✓ Read CSV files:")
        print(f"  - Header: {len(header_df)} rows")
        print(f"  - Transactions: {len(tx_df)} rows")
        print(f"  - Tx+IP flags: {len(tx_ip_df)} rows")
        print(f"  - Monthly income: {len(monthly_df)} rows")
    except FileNotFoundError as e:
        print(f"❌ Error reading CSV: {e}")
        return False
    
    # Prepare statement data
    statement_data = {
        'header_df': header_df,
        'tx_df': tx_df,
        'footer_df': footer_df,
        'meta_df': meta_df,
        'tx_ip_df': tx_ip_df,
        'monthly_income_df': monthly_df,
        'income_summary': income_summary_df.iloc[0].to_dict() if len(income_summary_df) > 0 else {},
        'client_iin': header_df.iloc[0].get('iin_bin') if 'iin_bin' in header_df.columns else None,
        'client_name': header_df.iloc[0].get('client') if 'client' in header_df.columns else None,
        'account_number': header_df.iloc[0].get('account') if 'account' in header_df.columns else None,
        'pdf_name': f"{stem}.pdf",
    }
    
    # Connect to database
    db = DatabaseConnection(**DB_CONFIG)
    try:
        db.connect()
        
        # Clean up test data from previous runs - delete all transactions_ip_flags without statements
        try:
            db.execute_query("""
                DELETE FROM transactions_ip_flags 
                WHERE transaction_id IN (
                    SELECT t.id FROM transactions t
                    WHERE t.statement_id IN (
                        SELECT id FROM statements WHERE bank = 'Alatau City Bank'
                    )
                )
            """)
        except:
            pass
        
        # Delete transactions
        try:
            db.execute_query("""
                DELETE FROM transactions 
                WHERE statement_id IN (
                    SELECT id FROM statements WHERE bank = 'Alatau City Bank'
                )
            """)
        except:
            pass
        
        # Delete statements (which cascades to other tables)
        try:
            db.execute_query("DELETE FROM statements WHERE bank = 'Alatau City Bank'")
        except:
            pass
        
        # Import statement
        statement_id = import_statement_to_db(db, statement_data, 'Alatau City Bank')
        
        print(f"\n✅ Statement successfully imported!")
        print(f"   Statement ID: {statement_id}")
        
        # Verify by querying
        results = db.execute_query(
            """
            SELECT c.full_name, a.account_number, s.bank, COUNT(t.id) as tx_count
            FROM statements s
            JOIN accounts a ON s.account_id = a.id
            JOIN clients c ON a.client_id = c.id
            LEFT JOIN transactions t ON s.id = t.statement_id
            WHERE s.id = %s
            GROUP BY c.full_name, a.account_number, s.bank
            """,
            (statement_id,)
        )
        
        if results:
            row = results[0]
            print(f"\n📊 Verification:")
            print(f"   Client: {row['full_name']}")
            print(f"   Account: {row['account_number']}")
            print(f"   Bank: {row['bank']}")
            print(f"   Transactions: {row['tx_count']}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        db.disconnect()


if __name__ == '__main__':
    print("=" * 60)
    print("Testing Alatau City Bank Statement Import")
    print("=" * 60)
    
    success = import_alatau_statement()
    
    print("=" * 60)
    if success:
        print("✅ Test completed successfully!")
    else:
        print("❌ Test failed!")
    print("=" * 60)
