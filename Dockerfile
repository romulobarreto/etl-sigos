FROM python:3.12-slim

# Instalar deps básicas que podem ser necessárias (NaN, psycopg2-binary, etc)
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY extraction ./extraction
COPY load ./load
COPY transformation ./transformation
COPY sql ./sql
COPY main.py .

CMD ["python", "main.py", "--report", "general", "--mode", "incremental"]