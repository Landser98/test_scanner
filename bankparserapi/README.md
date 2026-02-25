# bsvalidaiton (API)

FastAPI backend for bank statement parsing and analytics.

## What is included

- API service on port `8000`
- Swagger UI on `/docs`
- Statement parsing and analytics logic
- Database access layer and schema files

## Run locally (Python)

```bash
pip3 install -r requirements.txt
uvicorn src.api.app:app --host 0.0.0.0 --port 8000 --reload
```

Open:

- API: `http://localhost:8000`
- Swagger: `http://localhost:8000/docs`

## Run with Docker (external DB)

1. Fill infrastructure connection variables:

```bash
cp .env.example .env
# edit .env with provided DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD/SECRET_KEY
```

2. Start API:

```bash
docker compose up -d --build
```

Open:

- API: `http://localhost:8000`
- Swagger: `http://localhost:8000/docs`

Stop:

```bash
docker compose down
```

## Notes

- This repository contains only the API part.
- Upload UI and taxpayer search UI are split into separate repositories.
- DB is external and provided by infrastructure team; API connects using `.env`.
- If infrastructure requires TLS to PostgreSQL, set `DB_SSLMODE` (for example: `require`).
- Vault is supported: on startup the app can fetch JSON config from Vault KV v2 once and map keys to env vars.
- Configure Vault with `VAULT_*` variables from `.env.example` (token is read from env, not hardcoded).
