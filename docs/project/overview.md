# Visão geral

Este projeto automatiza a coleta de relatórios do SIGOS e mantém uma base paralela em Postgres (Supabase).

## Problema

- SIGOS fora do ecossistema de analytics (Snowflake).
- Rotina manual de download/limpeza/consolidação.

## Solução

Criar uma base paralela atualizada com duas estratégias:

- **Incremental:** recortes recentes (alta frequência).
- **Full:** reprocessamento completo (semanal) para capturar alterações retroativas.

## Resultados esperados

- Menos retrabalho
- Menos risco de erro
- Maior velocidade para criar e manter dashboards
