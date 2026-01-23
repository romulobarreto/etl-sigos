# Arquitetura

## Visão em alto nível

1. **ECS Fargate** executa um container Docker.
2. O container roda o ETL e baixa relatórios com Selenium (headless).
3. Os dados são transformados e carregados no **Supabase (Postgres)**.
4. O **EventBridge Scheduler** dispara execuções incremental e full.

## Componentes

- **Selenium + Chromium:** autentica e baixa relatórios
- **Transformação (Pandas):** limpeza, tipos, datas, padronização
- **Loader:** conexão Postgres e upsert/replace conforme modo

## Execução serverless

- Container no **ECR**
- Task no **ECS**
- Agendamento no **Scheduler**

!!! note
    A execução é event-driven e paga por uso: não há servidor para administrar.
