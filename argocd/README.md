# ArgoCD — Ingestor Orchestrator

Deploy GitOps com **External Secrets Operator** + ArgoCD.

O `ClusterSecretStore` `oci-vault` já existe (gerenciado via Terraform).

## Estrutura

```
argocd/
├── application.yaml
├── manifests/
│   └── external-secret.yaml      # ESO ExternalSecret → 4 secrets do Vault
└── README.md
```

## Deploy

```bash
kubectl apply -f argocd/application.yaml
```

Só isso. O ArgoCD gerencia tudo a partir do Git — inclusive o `ExternalSecret`.

## Como funciona

```
ClusterSecretStore "oci-vault" (Terraform) ──▶ OCI Vault
                                                    │
ExternalSecret (ArgoCD)                    ◀───────┘
      │
      ▼  K8s Secret "ingestor-orchestrator-csi"
      │
      ▼  Helm Chart (existingSecret)
      │
      ▼  env vars nos pods
```

## Secrets sincronizados

| Env var | OCID (sa-saopaulo-1) |
|---------|----------------------|
| `INGEST_ORCH_MYSQL_PASSWORD` | `amaaa...2zt6q` |
| `DUCKLAKE_CATALOG_URI` | `amaaa...abmxa` |
| `DUCKLAKE_DATA_PATH__ACCESS_KEY_ID` | `amaaa...6feiq` |
| `DUCKLAKE_DATA_PATH__SECRET_ACCESS_KEY` | `amaaa...ga52a` |
