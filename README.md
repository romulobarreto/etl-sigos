# ⚡ ETL SIGOS - Recuperação de Energia (CEEE Equatorial)

Projeto de **ETL (Extract, Transform, Load)** para automação da coleta de relatórios do sistema **SIGOS**, tratamento de dados e carga em um banco **Postgres** containerizado.  
Pensado para rodar em **Docker** com orquestração via **n8n** 🚀.

---

## 📂 Estrutura do Projeto

```
PIPELINE/
├── extraction/        # Scripts de extração (web scraping com Selenium)
│   ├── core/          # Configurações principais (navegador, utils)
│   └── reports/       # Extratores para relatórios específicos
├── transformation/    # Tratamento e normalização de DataFrames
├── load/              # Carregamento no Postgres
├── sql/               # Scripts SQL de inicialização
├── downloads/         # Relatórios baixados (volume no Docker)
├── logs/              # Logs de execução
├── main.py            # Entrada principal do ETL
├── Dockerfile         # Configuração da imagem Docker
├── docker-compose.yml # Orquestração dos serviços
├── requirements.txt   # Dependências Python
└── .env               # Variáveis de ambiente (NÃO versionar ⚠️)
```

---

## 🚀 Como rodar localmente (com Docker)

1. Crie um arquivo `.env` na raiz do projeto com as variáveis:

```env
SIGOS_USUARIO=seu_usuario
SIGOS_SENHA=sua_senha
HEADLESS=true

DB_HOST=db
DB_PORT=5432
DB_USER=etl_user
DB_PASS=senha123
DB_NAME=etl_sigos
```

⚠️ Importante: não suba este arquivo no GitHub.

2. Construa as imagens:

```bash
docker-compose build
```

3. Suba os serviços (Postgres, Adminer, ETL):

```bash
docker-compose up
```

4. Acesse o **Adminer** em [http://localhost:8080](http://localhost:8080)  
   - Sistema: PostgreSQL  
   - Servidor: `db`  
   - Usuário: `etl_user`  
   - Senha: `senha123`  
   - Banco: `etl_sigos`

---

## 🛠️ Executando o ETL manualmente

Por padrão o ETL roda com `general incremental`.  
Para executar outro relatório:

```bash
docker-compose run etl python main.py --report return --mode full
```

---

## 📊 Tecnologias usadas

- 🐍 **Python 3.12**  
- 📦 **Pandas / SQLAlchemy**  
- 🖥 **Selenium + Chrome Headless**  
- 🐘 **Postgres 16**  
- 📦 **Docker & Docker Compose**  
- 🧩 **Adminer** (interface DB)  
- 🔄 **N8N** *(orquestração)*  

---

## 📌 Próximos passos

- ✅ Finalizar dockerização do ETL  
- ⚙️ Configurar workflows no **n8n**  
- 📲 Integração com **Telegram** (logs, notificações)  
- 🗃️ Dashboards no **Power BI** / Metabase  

---

## ✨ Autor

Desenvolvido por **Rômulo** 🧑‍💻  
Analista Pleno @ CEEE Equatorial ⚡
