#!/bin/bash
# Скрипт для настройки базы данных PostgreSQL

set -e

echo "🔍 Проверка PostgreSQL..."

# Проверка установки PostgreSQL
if ! command -v psql &> /dev/null; then
    echo "❌ PostgreSQL не установлен!"
    echo "Установите PostgreSQL:"
    echo "  macOS: brew install postgresql@15"
    echo "  Linux: sudo apt-get install postgresql"
    exit 1
fi

# Проверка запуска PostgreSQL
if ! pg_isready -h localhost -p 5432 &> /dev/null; then
    echo "⚠️  PostgreSQL не запущен. Попытка запуска..."
    
    # Попытка запуска через brew services (macOS)
    if command -v brew &> /dev/null; then
        brew services start postgresql@15 2>/dev/null || brew services start postgresql 2>/dev/null || true
        sleep 2
    fi
    
    # Проверка снова
    if ! pg_isready -h localhost -p 5432 &> /dev/null; then
        echo "❌ Не удалось запустить PostgreSQL автоматически."
        echo "Запустите вручную:"
        echo "  macOS: brew services start postgresql@15"
        echo "  Linux: sudo systemctl start postgresql"
        exit 1
    fi
fi

echo "✅ PostgreSQL запущен"

# Проверка существования базы данных
DB_EXISTS=$(psql -U postgres -lqt 2>/dev/null | cut -d \| -f 1 | grep -qw bank_statements && echo "yes" || echo "no")

if [ "$DB_EXISTS" = "no" ]; then
    echo "📦 Создание базы данных bank_statements..."
    psql -U postgres -c "CREATE DATABASE bank_statements;" 2>/dev/null || {
        echo "⚠️  Не удалось создать БД с пользователем postgres. Пробую создать пользователя..."
        psql -U postgres -c "CREATE USER bank_user WITH PASSWORD 'secure_password';" 2>/dev/null || true
        psql -U postgres -c "CREATE DATABASE bank_statements OWNER bank_user;" 2>/dev/null || {
            echo "❌ Не удалось создать базу данных. Проверьте права доступа."
            exit 1
        }
    }
    echo "✅ База данных создана"
else
    echo "✅ База данных bank_statements уже существует"
fi

# Применение схемы
if [ -f "db/schema.sql" ]; then
    echo "📋 Применение схемы базы данных..."
    psql -U postgres -d bank_statements -f db/schema.sql 2>/dev/null || {
        echo "⚠️  Попытка с пользователем bank_user..."
        psql -U bank_user -d bank_statements -f db/schema.sql 2>/dev/null || {
            echo "❌ Не удалось применить схему. Проверьте права доступа."
            exit 1
        }
    }
    echo "✅ Схема применена"
else
    echo "⚠️  Файл db/schema.sql не найден"
fi

# Создание пользователя bank_user если не существует
USER_EXISTS=$(psql -U postgres -d bank_statements -tc "SELECT 1 FROM pg_roles WHERE rolname='bank_user'" 2>/dev/null | grep -q 1 && echo "yes" || echo "no")

if [ "$USER_EXISTS" = "no" ]; then
    echo "👤 Создание пользователя bank_user..."
    psql -U postgres -c "CREATE USER bank_user WITH PASSWORD 'secure_password';" 2>/dev/null || true
    psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE bank_statements TO bank_user;" 2>/dev/null || true
    psql -U postgres -d bank_statements -c "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO bank_user;" 2>/dev/null || true
    psql -U postgres -d bank_statements -c "GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO bank_user;" 2>/dev/null || true
    echo "✅ Пользователь создан"
fi

echo ""
echo "✅ База данных готова к использованию!"
echo ""
echo "Параметры подключения:"
echo "  Host: localhost"
echo "  Port: 5432"
echo "  Database: bank_statements"
echo "  User: bank_user"
echo "  Password: secure_password"
echo ""
echo "Для проверки подключения:"
echo "  psql -U bank_user -d bank_statements"
