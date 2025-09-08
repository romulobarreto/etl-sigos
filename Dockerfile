FROM python:3.12-slim

RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# instalar deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copiar código
COPY etl ./etl
COPY api ./api

# expor porta da API
EXPOSE 8000

# comando: roda a API (o ETL será chamado por subprocess)
CMD ["uvicorn", "api.app:app", "--host", "0.0.0.0", "--port", "8000"]