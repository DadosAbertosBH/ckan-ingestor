# ArgoCD — Ingestor Orchestrator

Deploy GitOps com **External Secrets Operator** + ArgoCD.

O `ClusterSecretStore` `oci-vault` já existe (gerenciado via Terraform).

## Estrutura

```
argocd/
├── application.yaml              # ArgoCD Application (multi-source)
├── manifests/
│   ├── oci-credentials.yaml      # Chave privada OCI (⚠️ SOPS antes do commit)
│   └── external-secret.yaml      # ESO ExternalSecret → 4 secrets do Vault
└── README.md
```

## Deploy

### 1. Preencher credencial OCI (se auth = user)

O `ClusterSecretStore` atual usa `auth.user = {}` (instance principal — OKE).
**No k3s isso não funciona.** É necessário mudar para `user` auth explícita.

Preencha `oci-credentials.yaml` com sua chave privada e atualize o `ClusterSecretStore`:

```hcl
auth = {
  user = {
    tenancy     = "<OCID>"
    user        = "<OCID>"
    fingerprint = "<FP>"
    privateKey = {
      name = "oci-api-key"
      key  = "key_file"
    }
  }
}
```

### 2. Criptografar com SOPS

```bash
sops --encrypt argocd/manifests/oci-credentials.yaml > argocd/manifests/oci-credentials.enc.yaml
rm argocd/manifests/oci-credentials.yaml
```

### 3. Aplicar

```bash
kubectl apply -f argocd/application.yaml
```

## Como funciona

```
ClusterSecretStore (Terraform) ──▶ OCI Vault
                                       │
ExternalSecret (ArgoCD)       ◀───────┘
      │
      ▼
K8s Secret "ingestor-orchestrator-csi"
      │
      ▼ existingSecret
Helm Chart → env vars nos pods
```

## Secrets sincronizados

| Env var | OCID (sa-saopaulo-1) |
|---------|----------------------|
| `INGEST_ORCH_MYSQL_PASSWORD` | `amaaa...2zt6q` |
| `DUCKLAKE_CATALOG_URI` | `amaaa...abmxa` |
| `DUCKLAKE_DATA_PATH__ACCESS_KEY_ID` | `amaaa...6feiq` |
| `DUCKLAKE_DATA_PATH__SECRET_ACCESS_KEY` | `amaaa...ga52a` |
