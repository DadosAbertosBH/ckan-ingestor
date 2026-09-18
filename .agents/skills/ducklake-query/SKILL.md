---
name: ducklake-query
description: Query DuckLake data using DuckDB CLI over the Tailscale connection. Use this when the user wants to inspect, query, or explore data stored in the DuckLake catalog.
---

# DuckLake Query

Query DuckLake from the local DuckDB CLI (`/opt/homebrew/bin/duckdb`) by connecting to the
catalog over **Tailscale** (no `kubectl port-forward` needed). The catalog is the Postgres
cluster in the `orchestrator` namespace, exposed to the tailnet, and the data files live in
Cloudflare R2.

The host machine must be connected to the tailnet (`tailscale status` should list the
`orchestrator-ingestor-orchestrator-postgres-tailscale` node). Commands reach the tailnet over
raw TCP, so they need unrestricted outbound access (not just an HTTP proxy grant).

## Credentials

Never hard-code the cluster secrets. Read them on demand:

```bash
# Postgres catalog password (user "app", database "app")
kubectl get secret ingestor-orchestrator-postgres-app -n orchestrator \
  -o jsonpath='{.data.password}' | base64 -d

# R2 (S3-compatible) credentials for the DuckLake data path
kubectl get secret ingestor-orchestrator-csi -n orchestrator \
  -o jsonpath='{.data.DUCKLAKE_DATA_PATH__ACCESS_KEY_ID}' | base64 -d
kubectl get secret ingestor-orchestrator-csi -n orchestrator \
  -o jsonpath='{.data.DUCKLAKE_DATA_PATH__SECRET_ACCESS_KEY}' | base64 -d
```

Substitute the three values into `<PG_PASSWORD>`, `<R2_ACCESS_KEY_ID>` and
`<R2_SECRET_ACCESS_KEY>` below. Because the terminal forbids shell substitutions, run the
`kubectl` lookups first and paste the literal values.

## How to Connect

Replace `<QUERY>` with your SQL. The S3 settings must be set **before** the `ATTACH`.

```bash
duckdb :memory: -c "SET s3_url_style='path'; SET s3_use_ssl=true; SET s3_region='auto'; SET s3_endpoint='d8915a6c9b1e24b2ded7d9cd72318cdd.r2.cloudflarestorage.com'; SET s3_access_key_id='<R2_ACCESS_KEY_ID>'; SET s3_secret_access_key='<R2_SECRET_ACCESS_KEY>'; INSTALL postgres; INSTALL httpfs; INSTALL ducklake; LOAD postgres; LOAD httpfs; LOAD ducklake; ATTACH IF NOT EXISTS 'ducklake:postgres:dbname=app host=orchestrator-ingestor-orchestrator-postgres-tailscale.tail49b842.ts.net port=5432 user=app password=<PG_PASSWORD>' AS lake (DATA_PATH 's3://public-datasets', DATA_INLINING_ROW_LIMIT 10000); USE lake; <QUERY>"
```

## Connection Parameters

| Setting | Value |
|---------|-------|
| Catalog host | `orchestrator-ingestor-orchestrator-postgres-tailscale.tail49b842.ts.net` |
| Catalog port | `5432` |
| Catalog database | `app` |
| Catalog user | `app` |
| Catalog password | secret `ingestor-orchestrator-postgres-app` key `password` |
| S3 (R2) endpoint | `d8915a6c9b1e24b2ded7d9cd72318cdd.r2.cloudflarestorage.com` |
| S3 bucket | `public-datasets` |
| S3 access key | secret `ingestor-orchestrator-csi` key `DUCKLAKE_DATA_PATH__ACCESS_KEY_ID` |
| S3 secret key | secret `ingestor-orchestrator-csi` key `DUCKLAKE_DATA_PATH__SECRET_ACCESS_KEY` |
| S3 use SSL | `true` |
| S3 region | `auto` |
| S3 URL style | `path` |

These mirror the cluster ConfigMap `ingestor-orchestrator` (`DUCKLAKE_DATA_PATH__*`) and the
`DUCKLAKE_*` env vars of the orchestrator/worker pods.

## Useful Queries

The catalog is large (~17k resource tables) and the bucket holds a very large number of parquet
objects. Scope every query.

Find the tables you care about instead of listing all of them:

```sql
SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'ckan_%' ORDER BY 1;
```

Count synced resources (metadata table, lives in the catalog):

```sql
SELECT COUNT(*) FROM ckan_resource;
```

Preview synced resources:

```sql
SELECT id, name, format, last_modified FROM ckan_resource WHERE id = '<resource_id>';
```

Check whether a resource version was already processed (the ingestion ledger):

```sql
SELECT * FROM ckan_resource_last_update WHERE ckan_resource_id = '<resource_id>';
```

Read one resource table (a resource's table name is its resource id):

```sql
SELECT * FROM "<resource_id>" LIMIT 10;
```

## Tips

- Run from the project root directory.
- Use `timeout_ms` of at least 120000: extension loading plus a remote catalog can be slow.
- Do **not** run unbounded `glob('s3://public-datasets/**')` or full `SHOW TABLES` — the bucket
  and table list are huge and the command will hang. Always filter.
- `LOAD` skips the extension download when it is already installed; keep `INSTALL` for the first
  run only if you prefer, but it is harmless.
