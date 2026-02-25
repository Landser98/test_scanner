# bank-parser-upload-ui

Streamlit UI for uploading and processing bank statements.

## What is included

- Streamlit upload UI on port `8502`
- UI workflow and rendering components
- Supporting parsing modules required by the upload flow

## Run locally (Python)

```bash
pip3 install -r requirements.txt
python3 run_upload_ui.py
```

Or directly:

```bash
streamlit run src/ui/upload_app.py --server.port=8502 --server.address=0.0.0.0 --server.headless=true
```

Open:

- Upload UI: `http://localhost:8502`

## Run with Docker (external DB + API)

1. Fill infrastructure connection variables:

```bash
cp .env.example .env
# edit .env with provided DB_* and API_BASE_URL
# set SECRET_KEY and ALLOWED_ORIGINS for secure API defaults
```

2. Start Upload UI:

```bash
docker compose up -d --build
```

Open:

- Upload UI: `http://localhost:8502`

Stop:

```bash
docker compose down
```

## Notes

- This repository contains only the upload Streamlit UI part.
- Taxpayer search UI is intentionally excluded.
- API health is shown in sidebar using `API_BASE_URL` from `.env`.
- If infrastructure requires TLS to PostgreSQL, set `DB_SSLMODE` (for example: `require`).
- Vault is supported: on startup the app can fetch JSON config from Vault KV v2 once and map keys to env vars.
- Configure Vault with `VAULT_*` variables from `.env.example` (token is read from env, not hardcoded).
