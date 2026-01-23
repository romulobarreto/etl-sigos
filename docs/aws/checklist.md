# Checklist de recursos na AWS

## ECR

- Repositório de imagem do container

## ECS

- Cluster
- Task Definition (Fargate / awsvpc)
- Logs (CloudWatch)

## EventBridge Scheduler

- Schedule incremental
- Schedule full

## IAM

- Role de execução para o Scheduler (`scheduler.amazonaws.com`)
- Permissões para `ecs:RunTask` e `iam:PassRole`
