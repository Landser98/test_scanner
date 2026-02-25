FROM python:3.12-slim

# Security: Create non-root user (fixes "Wrong user settings")
RUN groupadd -r appuser && useradd -r -g appuser appuser

RUN apt-get update && apt-get install -y \
    ghostscript \
    python3-tk \
    libgl1 \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .

# Install all deps from requirements only (no hardcoded package names)
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY . .

# Security: Run as non-root
RUN chown -R appuser:appuser /app
USER appuser

EXPOSE 8502

CMD ["python", "-m", "streamlit", "run", "src/ui/upload_app.py", "--server.port=8502", "--server.address=0.0.0.0", "--server.headless=true"]