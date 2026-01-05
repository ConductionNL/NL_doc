# Secrets die handmatig in het cluster moeten worden aangemaakt

## MinIO credentials

```bash
kubectl create secret generic minio-credentials -n nldoc \
  --from-literal=rootUser=minioadmin \
  --from-literal=rootPassword=$(openssl rand -base64 24)
```

## NLdoc basic auth (voor ingress)

```bash
# Genereer htpasswd
htpasswd -c auth admin
# Of met openssl:
echo "admin:$(openssl passwd -apr1 'jouw-wachtwoord')" > auth

kubectl create secret generic nldoc-basic-auth -n nldoc --from-file=auth
```

## Automatisch gegenereerde secrets

Deze worden automatisch aangemaakt door operators:

- `rabbitmq-default-user` - door RabbitMQ Cluster Operator
- `nldoc.nldoc-postgres.credentials.postgresql.acid.zalan.do` - door Zalando Postgres Operator

