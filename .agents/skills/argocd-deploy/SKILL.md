---
name: argocd-deploy
description: Troubleshoot ArgoCD deployments using kubectl (CRDs) and argocd CLI. Use this when the user asks about sync status, application health, SharedResourceWarnings, or deploy issues on the cluster.
---

# ArgoCD Deploy Troubleshooting

Access ArgoCD via kubectl (primary) or argocd CLI. The cluster runs on k3s via Tailscale.

## ArgoCD Server

- URL: `http://argocd-argocd-lb.tail49b842.ts.net/`
- argocd CLI context: `argocd-argocd-lb.tail49b842.ts.net` (add via `argocd login`)

## ArgoCD Context Setup

```bash
# Login (first time)
argocd login argocd-argocd-lb.tail49b842.ts.net --grpc-web --insecure

# Switch context
argocd context argocd-argocd-lb.tail49b842.ts.net
```

## kubectl — Always Available

Use kubectl when argocd CLI has issues (socket binding, network). All ArgoCD resources are CRDs.

### Application status

```bash
# List all applications
kubectl get applications -n argocd

# Full manifest
kubectl get application -n argocd <NAME> -o json

# Sources (which repos/paths)
kubectl get application -n argocd <NAME> -o jsonpath='{.spec.sources[*].repoURL}'

# Sync status
kubectl get application -n argocd <NAME> -o jsonpath='{.status.sync.status}'

# Health
kubectl get application -n argocd <NAME> -o jsonpath='{.status.health.status}'

# Conditions (warnings, errors)
kubectl get application -n argocd <NAME> -o json | python3 -c "
import sys,json
a=json.load(sys.stdin)
for c in a.get('status',{}).get('conditions',[]):
    if c['type'] != 'SharedResourceWarning': continue
    print(f'{c[\"type\"]}: {c[\"message\"]}')
"

# Managed resources
kubectl get application -n argocd <NAME> -o jsonpath='{range .status.resources[*]}{.kind}/{.name} ({.namespace}) status={.status}{"\n"}{end}'
```

### ApplicationSet

```bash
kubectl get applicationsets -n argocd
kubectl get applicationset -n argocd <NAME> -o json
```

### Sync an application

```bash
# Via kubectl annotation (triggers sync)
kubectl patch application -n argocd <NAME> --type merge -p '{"operation":{"sync":{}}}'

# Via argocd CLI
argocd app sync <NAME> --prune
```

### Hard refresh (re-read Git)

```bash
kubectl patch application -n argocd <NAME> --type merge -p '{"metadata":{"annotations":{"argocd.argoproj.io/refresh":"hard"}}}'
```

## Common Issues

### SharedResourceWarning

Two Applications claim the same resource. Check:

```bash
kubectl get application -n argocd <NAME> -o jsonpath='{.status.conditions[?(@.type=="SharedResourceWarning")].message}'
```

Fix: remove the duplicate source/path from one Application.

### OutOfSync

Application spec diverged from live state. Check what changed:

```bash
kubectl get application -n argocd <NAME> -o jsonpath='{.status.sync.revision}' && echo
```

Force sync: patch with `{"operation":{"sync":{"prune":true}}}`

### GitOps repos

| Repo | Purpose |
|------|---------|
| `pedalin/ckan-ingestor` | Application source (Helm chart) |
| `noctcloud/argocd-applications` | GitOps config (Applications, ExternalSecrets) |

## Cluster context

- `orchestrator` namespace: app workloads
- `argocd` namespace: ArgoCD + Crossplane resources
- k3s nodes: `k3sserver0` through `k3sserver3`
- Tailscale network: `tail49b842.ts.net`
