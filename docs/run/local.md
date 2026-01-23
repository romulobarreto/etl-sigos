# Rodar localmente (dev/debug)

Mesmo que o SIGOS exija credenciais internas, rodar localmente é útil para desenvolvimento e debugging.

## Passos

1. Instalar dependências

```bash
poetry install
```

2. Configurar `.env`

3. Executar

```bash
task cycle_inc
task cycle_full
```

!!! Warning
    Não versione o `.env`.
