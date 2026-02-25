#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Тест подключения к базе данных
"""

import sys
from pathlib import Path

# Добавляем корень проекта в путь
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.db.database import DatabaseConnection
from src.db.config import DB_CONFIG

def test_connection():
    """Тест подключения к БД"""
    print("🔍 Тестирование подключения к базе данных...")
    print(f"   Host: {DB_CONFIG['host']}")
    print(f"   Port: {DB_CONFIG['port']}")
    print(f"   Database: {DB_CONFIG['database']}")
    print(f"   User: {DB_CONFIG['user']}")
    print()
    
    try:
        db = DatabaseConnection(**DB_CONFIG)
        db.connect()
        print("✅ Подключение успешно!")
        
        # Проверка таблиц
        cursor = db.connection.cursor()
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        tables = cursor.fetchall()
        
        if tables:
            print(f"\n📋 Найдено таблиц: {len(tables)}")
            for table in tables[:10]:  # Показываем первые 10
                print(f"   - {table[0]}")
            if len(tables) > 10:
                print(f"   ... и еще {len(tables) - 10}")
        else:
            print("⚠️  Таблицы не найдены. Примените схему: psql -U postgres -d bank_statements -f db/schema.sql")
        
        # Проверка клиентов
        cursor.execute("SELECT COUNT(*) FROM clients;")
        client_count = cursor.fetchone()[0]
        print(f"\n👥 Клиентов в БД: {client_count}")
        
        # Проверка выписок
        cursor.execute("SELECT COUNT(*) FROM statements;")
        statement_count = cursor.fetchone()[0]
        print(f"📄 Выписок в БД: {statement_count}")
        
        cursor.close()
        db.disconnect()
        
        print("\n✅ База данных готова к использованию!")
        return True
        
    except Exception as e:
        print(f"\n❌ Ошибка подключения: {e}")
        print("\nПроверьте:")
        print("  1. PostgreSQL запущен: pg_isready")
        print("  2. База данных создана: psql -U postgres -l | grep bank_statements")
        print("  3. Схема применена: psql -U postgres -d bank_statements -f db/schema.sql")
        print("  4. Параметры подключения в src/db/config.py")
        return False

if __name__ == "__main__":
    success = test_connection()
    sys.exit(0 if success else 1)
