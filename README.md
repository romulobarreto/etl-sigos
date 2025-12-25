# ⚡ ETL SIGOS - Recuperação de Energia (CEEE Equatorial)

Projeto de **ETL (Extract, Transform, Load)** para automação da coleta de relatórios do sistema **SIGOS**, tratamento dos dados e carga em um banco **Postgres (Supabase)**.

> Hoje o ETL roda **localmente e de forma manual** na máquina do analista.  

---

## 📂 Estrutura do Projeto

```bash
PIPELINE/
├── extraction/        # Scripts de extração (web scraping com Selenium)
│   ├── core/          # Configurações principais (navegador, utils)
│   └── reports/       # Extratores para relatórios específicos (general, return, etc.)
├── transformation/    # Tratamento e normalização de DataFrames
├── load/              # Rotinas de carga para o Postgres (Supabase)
├── sql/               # (Opcional) Scripts SQL de apoio
├── downloads/         # Relatórios baixados do SIGOS
├── logs/              # Logs de execução
├── main.py            # Entrada principal do ETL (CLI)
├── requirements.txt   # Dependências Python
└── .env               # Variáveis de ambiente (NÃO versionar ⚠️)
```
---

## ✅ Pré-requisitos

Para rodar o ETL na máquina local você precisa de:

- 🐍 **Python 3.10+**  
- 🌐 **Google Chrome** instalado  
- 🧩 **ChromeDriver / WebDriver Manager** (já tratado via código, já está no `requirements.txt`)  
- Acesso ao:
  - Sistema **SIGOS** (usuário e senha)
  - Banco **Supabase (Postgres)**

---

## 🔐 Configuração do `.env`

Crie um arquivo `.env` na raiz do projeto com algo nesse formato:

```env
# Credenciais SIGOS
SIGOS_USUARIO=seu_usuario
SIGOS_SENHA=sua_senha
HEADLESS=true  # true = sem abrir janela do Chrome / false = abre navegador

# Conexão com o banco (Supabase / Postgres)
DB_HOST=seu_host_supabase
DB_PORT=5432
DB_NAME=nome_do_banco
DB_USER=usuario
DB_PASS=senha_super_secreta

# (Opcional) Outras configs de log / diretórios, se existirem no código
LOG_LEVEL=INFO
```

⚠️ **Importante:**  
- Não versionar o `.env` no GitHub.  
- Se estiver usando uma `DATABASE_URL` única do Supabase, você pode ter algo como:

```env
DATABASE_URL=postgresql://usuario:senha@host:5432/nome_do_banco
```

e o código usa essa variável diretamente.

---

## 🧪 Como rodar o projeto localmente

1. **Criar e ativar o ambiente virtual**

```bash
# Dentro da pasta do projeto
python -m venv .venv

# Windows
.venv\Scriptsctivate

# Linux / WSL / macOS
source .venv/bin/activate
```

2. **Instalar as dependências**

```bash
pip install -r requirements.txt
```

3. **Confirmar que o `.env` está criado** na raiz do projeto, com as variáveis certas.

4. **Rodar o ETL**

O `main.py` expõe uma CLI onde você escolhe:

- o tipo de relatório (`--report`)
- o modo (`--mode`), por exemplo `full` ou `incremental`.

Exemplos:

```bash
# Relatório "general" em modo incremental (fluxo usado no dia a dia)
python main.py --report general --mode incremental

# Relatório "general" em modo full (reprocessa toda a base)
python main.py --report general --mode full

# Relatório "return" em modo full
python main.py --report return --mode full
```

Durante a execução, o fluxo é:

1. **Extract**  
   - Faz login no SIGOS com Selenium  
   - Navega até o relatório desejado  
   - Baixa o arquivo (CSV/XLSX) para a pasta `downloads/`

2. **Transform**  
   - Lê os arquivos baixados com Pandas  
   - Normaliza nomes de colunas, tipos, datas (formato pt-BR → ISO)  
   - Faz tratamentos específicos por relatório (deduplicação, limpeza, etc.)

3. **Load**  
   - Conecta ao banco Postgres (Supabase) usando as variáveis do `.env`  
   - Insere/atualiza os dados nas tabelas-alvo  
   - Em modo `incremental`, só processa o recorte configurado (ex.: últimos dias / mês corrente)

Os logs das execuções ficam na pasta `logs/` (se configurado no código).

---

## 🧱 Tecnologias usadas

- 🐍 **Python 3.x**
- 📦 **Pandas / SQLAlchemy**
- 🖥 **Selenium + Chrome Headless**
- 🐘 **Postgres (Supabase)**
- 📁 **.env** para gerenciamento de credenciais
- 📝 **Logging** para acompanhamento das execuções

---

## 🗺️ Roadmap / Futuro

> Coisas planejadas mas **ainda não implementadas na prática**:

- Containerização com **Docker** (ETL + banco local + Adminer)  
- Orquestração com **n8n** ou outro scheduler (rodar em horários fixos)  
- Notificações (ex.: Telegram) com resumo dos resultados  
- Publicação automática em um banco dedicado para **dashboards (Power BI / Metabase)**

---

## ✨ Autor

Desenvolvido por **Rômulo** 🧑‍💻  
Analista Pleno @ **CEEE Equatorial** ⚡
