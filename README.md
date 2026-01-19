# ⚡ ETL SIGOS — Base paralela (PostgreSQL/Supabase) com atualização automática

> **Contexto rápido:** o SIGOS é um sistema crítico do dia a dia, mas o banco original fica em um servidor interno da Equatorial ao qual eu não tenho acesso direto. A empresa migrou para o **Snowflake**, porém o **SIGOS ficou fora desse ecossistema** — e isso travava (muito) o trabalho de análise.

## 😵 A dor (real)

Antes deste projeto, para acompanhar indicadores diários/semanais/mensais era necessário:

- entrar no SIGOS
- baixar CSV manualmente (vários relatórios)
- limpar/ajustar na mão
- juntar bases e publicar dashboard

Resultado: **tempo perdido**, retrabalho e risco de erro.

## ✅ O que este projeto resolve

A ideia foi **clonar o banco “na prática”**, criando uma **base paralela** sempre atualizada:

- 🔁 **Incremental (hora em hora):** baixa dados recentes de 2 - 6 meses para manter o banco sempre atualizado.
- 🧹 **Full (semanal):** aos domingos reprocessa tudo, baixa todas as tabelas novamente, porque durante a semana pode acontecer **auditoria/ajuste de registros antigos** — e isso não seria capturado por um incremental “curto”.

Com isso, eu consigo criar e automatizar controles e relatórios **sem depender de baixar CSV na mão**.

## 📊 Quais dados entram no banco?

Hoje o ETL mantém duas grandes bases:

- **`general_reports`**: Qualquer serviço protocolado (qualquer status) entra aqui. É a base para visão geral do processo de recuperação de energia.
- **`return_reports`**: Serviços que voltam para campo por inconsistência/erro/incompletude — base essencial para acompanhar retrabalho e qualidade.

## 🧠 Arquitetura (visão técnica)

- 🕷️ **Extract:** Selenium + Chromium (headless) para autenticar e baixar relatórios.
- 🧽 **Transform:** limpeza, normalização e padronização (ex.: limpeza de registros duplicados,  definição de serviços que são da regional norte / sul e definição de serviços que são de alta / baixa tensão).
- 🐘 **Load:** carga em **PostgreSQL (Supabase)**.
- ☁️ **Run:** container Docker executando em **AWS ECS Fargate**.
- ⏰ **Schedule:** **EventBridge Scheduler** (incremental e full) para automação.

> O pipeline roda em Fargate (serverless): **sem servidor para administrar** e pagando basicamente por execução.

## 🗂️ Estrutura do repositório

```text
etl-sigos/
  data/
  etl/
    downloads/
    extraction/
      core/
      reports/
    load/
      loader.py
    sql/
      init_tables.sql
    transformation/
      transformer.py
    main.py
  logs/
  tests/
    test_data_quality.py
    test_loader.py
    test_transformer.py
  docs/
  mkdocs.yml 
  Dockerfile
  docker-compose.yml
  pyproject.toml
  poetry.lock
  README.md
```

## 🔐 Variáveis de ambiente

Este projeto usa variáveis de ambiente para credenciais do SIGOS e conexão com o banco.

Exemplo (não versionar):

```env
# SIGOS
SIGOS_USUARIO=...
SIGOS_SENHA=...
HEADLESS=true

# Banco (Supabase Postgres)
DB_HOST=...
DB_NAME=...
DB_USER=...
DB_PASS=...
DB_PORT=6543
```

## ▶️ “Como rodar” — faz sentido se ninguém tem acesso ao SIGOS?

Sim — e boa pergunta.

Mesmo que um entusiasta do projeto não consiga executar (sem credenciais), essa seção serve para mostrar que:

- o projeto é **reprodutível**
- existe um “caminho padrão” para rodar/testar

Ou seja: não é tutorial para “usuário final”, é **documentação técnica**.

### Rodar localmente (dev/debug)

```bash
poetry install
poetry run python etl/main.py --cycle-incremental
```

> Dica: local é ótimo para debugar scraping/transformações. Em produção, a execução oficial acontece na AWS.

## ☁️ Execução na AWS (produção)

- Imagem Docker publicada no **ECR**
- Task definida no **ECS (Fargate)**
- Agendamento via **EventBridge Scheduler**:
  - `etl-sigos-incremental` (execução recorrente)
  - `etl-sigos-full` (execução semanal)

## 🧪 Testes

A pasta `tests/` contém testes de qualidade de dados e componentes principais.

```bash
poetry run pytest
```

## 🧱 Próximos passos (de engenharia)

Este projeto está “pronto” para o objetivo atual.

Evolução planejada (como **outro projeto/etapa**):

- modelagem em camadas **Bronze / Prata / Ouro** (ex.: Databricks)
- mover o destino do Supabase para um ambiente de analytics

## ✍️ Autor

**Rômulo Barreto da Silva** — Analista Pleno @ CEEE Equatorial ⚡

