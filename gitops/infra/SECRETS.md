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

## Argo CD repository secrets (voor private GitLab Helm registries)

De `Application` verwijst naar Helm registries op GitLab (editor-app, api, kimi). Als deze privé zijn, moet Argo CD authenticeren met een GitLab Personal Access Token (scope: `read_api` minimaal).

Maak in de `argocd` namespace per registry een repository-secret aan met onderstaande YAML (pas `username` en `password` aan en voer elke secret apart toe).

Voorbeeld (opslaan als `repo-editor-app.yaml` en toepassen met `kubectl -n argocd apply -f repo-editor-app.yaml`):

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: repo-editor-app
  namespace: argocd
  labels:
    argocd.argoproj.io/secret-type: repository
type: Opaque
stringData:
  url: https://gitlab.com/api/v4/projects/68643351/packages/helm/stable
  username: <gitlab-username>
  password: <personal-access-token-met-read_api>
```

API chart (opslaan als `repo-api.yaml`):

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: repo-api
  namespace: argocd
  labels:
    argocd.argoproj.io/secret-type: repository
type: Opaque
stringData:
  url: https://gitlab.com/api/v4/projects/68640094/packages/helm/stable
  username: <gitlab-username>
  password: <personal-access-token-met-read_api>
```

Kimi chart (opslaan als `repo-kimi.yaml`):

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: repo-kimi
  namespace: argocd
  labels:
    argocd.argoproj.io/secret-type: repository
type: Opaque
stringData:
  url: https://gitlab.com/api/v4/projects/68736728/packages/helm/stable
  username: <gitlab-username>
  password: <personal-access-token-met-read_api>
```

Na het aanmaken:

```bash
kubectl -n argocd get secret repo-editor-app repo-api repo-kimi -o yaml | grep argocd.argoproj.io/secret-type
```

Daarna kun je in Argo CD de app `nldoc` opnieuw syncen. Als alternatief kun je de charts ook naar jullie eigen registry mirroren en de `repoURL`’s in `gitops/apps/nldoc/application.yaml` daarop bijwerken.

