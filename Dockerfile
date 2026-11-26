FROM python:3.11-slim

# Sistem bağımlılıkları
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV PORT=8080

# Bağımlılıkları tek seferlik önbelleğe al
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Uygulama kodlarını kopyala
COPY . .

EXPOSE 8080

# Google Cloud Run için FastAPI sunucusunu başlat
CMD exec uvicorn main:app --host 0.0.0.0 --port ${PORT}
