# ETL SIGOS

> Base paralela (PostgreSQL/Supabase) com extração automatizada via Selenium e execução serverless na AWS.

## Por que isso existe?

O banco original do SIGOS fica em um servidor interno sem acesso direto para análise. Isso forçava um processo manual: baixar CSVs, tratar na mão e atualizar relatórios continuamente.

Este projeto cria uma **base paralela** sempre atualizada no **Supabase (Postgres)**, permitindo que dashboards e relatórios sejam construídos sem depender de downloads manuais.

## O que ele faz

- **Incremental (recorrente):** atualiza o banco com os dados recentes.
- **Full (semanal):** reprocessa tudo para capturar correções e auditorias em registros antigos.

## Principais datasets

- **general_reports**: todos os serviços protocolados (qualquer status).
- **return_reports**: serviços que entraram em status de retorno (retrabalho/qualidade).

## Stack

- Python + Selenium (Chromium)
- Docker
- AWS ECS Fargate + EventBridge Scheduler
- Supabase (PostgreSQL)

!!! tip "Dica"
    Se você chegou aqui por portfólio: o foco é mostrar um pipeline completo (extract/transform/load) com automação, logs e testes.
