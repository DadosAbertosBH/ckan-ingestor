---
name: iggy-cluster
description: Inspect or manage Apache Iggy streams and topic partitions in the Noct Kubernetes cluster through Tailscale. Use for Iggy topology, partition, or CLI operations in this project.
---

# Iggy Cluster

The Noct cluster is reachable through the Tailscale tailnet. Use the version-matched
Iggy CLI installed in the running server pod rather than installing a local CLI.

## Connect and inspect

1. Confirm `tailscale status --json` reports `BackendState` as `Running`.
2. Confirm the active Kubernetes context is `noct`, then inspect the ArgoCD application
   and the Iggy pod in namespace `orchestrator`.
3. Run the CLI in the deployment with its existing secret-backed credentials. Keep the
   password out of command output and shell history:

```bash
kubectl exec -n orchestrator deploy/iggy -- sh -lc \
  'iggy -u "$IGGY_ROOT_USERNAME" -p "$IGGY_ROOT_PASSWORD" topic get ckan-ingestor <topic>'
```

The application stream is `ckan-ingestor`; its primary topics are `jobs`,
`jobs-retry`, and `job-results`.

## Partition changes

Before changing a topic, read its current count and calculate the additive delta.
`iggy partition create` takes the number of partitions to add, rather than the desired
total. For example, a topic at 3 partitions needs `partition create ... 3` to reach 6:

```bash
kubectl exec -n orchestrator deploy/iggy -- sh -lc \
  'iggy -u "$IGGY_ROOT_USERNAME" -p "$IGGY_ROOT_PASSWORD" \
  partition create ckan-ingestor jobs 3'
```

Use explicit user authorization before a topology mutation. After a successful change,
query the topic again and report its final count. If the current count already meets the
target, report that state. If it exceeds the target, stop and request direction because
the safe operation is an additive expansion.

## GitOps boundary

Topic data is changed through the Iggy CLI. Helm values, Application versions, and
Kubernetes resources remain managed through Git and ArgoCD: edit, commit, push, and
then verify `Synced` and `Healthy`. Do not patch ArgoCD-managed resources directly.
