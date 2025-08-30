# Usa Python 3.12 slim (leve e estável)
FROM python:3.12-slim

# Instala dependências do sistema (pro Chrome headless do Selenium)
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    gnupg \
    curl \
    libglib2.0-0 \
    libnss3 \
    libgconf-2-4 \
    libxi6 \
    libxcursor1 \
    libxrandr2 \
    libxss1 \
    libxtst6 \
    xvfb \
    fonts-liberation \
    libappindicator3-1 \
    xdg-utils \
    libu2f-udev \
    && rm -rf /var/lib/apt/lists/*

# Baixa e instala o Chrome estável
RUN wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt-get install -y ./google-chrome-stable_current_amd64.deb \
    && rm google-chrome-stable_current_amd64.deb

# Define diretório de trabalho
WORKDIR /app

# Copia requirements e instala dependências Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia apenas o que interessa pro container
COPY extraction ./extraction
COPY core ./core
COPY load ./load
COPY transformation ./transformation
COPY sql ./sql
COPY main.py .

# Cria volumes para persistir downloads/logs
VOLUME [ "/app/downloads", "/app/logs" ]

# Entry-point padrão
CMD ["python", "main.py", "--report", "general", "--mode", "incremental"]