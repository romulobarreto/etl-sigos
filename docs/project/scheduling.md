# Agendamentos (incremental / full)

## Por que dois modos?

Durante a semana podem ocorrer correções em dados antigos (auditorias). Um incremental curto não capturaria isso.

- **Incremental:** deixa a base atualizada para relatórios do dia a dia.
- **Full:** garante consistência semanal.

## Como funciona na AWS

Os agendamentos são configurados no **EventBridge Scheduler**, que dispara uma Task no ECS com override de comando:

- incremental: `python etl/main.py --cycle-incremental`
- full: `python etl/main.py --cycle-full`
