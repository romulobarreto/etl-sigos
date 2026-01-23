# Troubleshooting

## Erro: `Unable to convert target input to TaskOverride`

Causa comum: o Scheduler exige que o `Input` seja uma **string JSON escapada**.

Solução: gerar o arquivo `target.json` com `Input` no formato:

```json
"Input": "{\"containerOverrides\":[{\"name\":\"etl-sigos\",\"command\":[\"python\",\"etl/main.py\",\"--cycle-incremental\"]}]}"
```

## Erro: `The execution role you provide must allow AWS EventBridge Scheduler to assume the role`

Causa: Trust policy incorreta.

Solução: a role precisa permitir `scheduler.amazonaws.com`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {"Service": "scheduler.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }
  ]
}
```

## Selenium / Chrome

- Se travar no login, valide modo headless e versões do Chromium/driver.
- Confira logs da Task no CloudWatch.
