---
name: ducklake-query
description: Query DuckLake data using DuckDB CLI inside the Docker environment. Use this when the user wants to inspect, query, or explore data stored in the DuckLake catalog.
---

# DuckLake Query

Query DuckLake data using the local DuckDB CLI (`/opt/homebrew/bin/duckdb`). The Docker services are exposed on localhost with mapped ports.

## How to Connect

Use the following DuckDB CLI invocation. Replace `<QUERY>` with your SQL:

```bash
duckdb :memory: -c "SET s3_url_style='path'; SET s3_use_ssl=false; SET s3_endpoint='localhost:9100'; SET s3_access_key_id='admin'; SET s3_secret_access_key='password'; INSTALL postgres; INSTALL httpfs; LOAD ducklake; LOAD postgres; LOAD httpfs; SET pg_debug_show_queries=false; ATTACH IF NOT EXISTS 'ducklake:postgres:dbname=ducklake_catalog host=localhost port=5433 user=postgres password=postgres' AS lake (DATA_PATH 's3://warehouse', DATA_INLINING_ROW_LIMIT 10000, AUTOMATIC_MIGRATION TRUE); USE lake; <QUERY>"
```

## Port Mapping

The `.env` uses Docker service hostnames. When connecting locally, map as follows:

| .env Value | Local Value | Docker Port Mapping |
|------------|-------------|---------------------|
| `rustfs:9000` | `localhost:9100` | `9100:9000` |
| `postgres` (host) | `localhost` | `5433:5432` |
| `mysql` (host) | `localhost` | `3306:3306` |

## Connection Parameters

| Setting | Local Value | Env Var |
|---------|-------------|---------|
| Catalog URI | `postgres:dbname=ducklake_catalog host=localhost port=5433 user=postgres password=postgres` | `DUCKLAKE_CATALOG_URI` |
| S3 Endpoint | `localhost:9100` | `S3_ENDPOINT` |
| S3 Access Key | `admin` | `S3_ACCESS_KEY_ID` |
| S3 Secret Key | `password` | `S3_SECRET_ACCESS_KEY` |
| S3 Bucket | `warehouse` | `S3_BUCKET` |
| S3 Use SSL | `false` | `S3_USE_SSL` |
| S3 URL Style | `path` | `S3_URL_STYLE` |

## Useful Queries

List all tables:
```sql
SHOW TABLES;
```

Count resources:
```sql
SELECT COUNT(*) FROM ckan_resource;
```

Preview synced resources:
```sql
SELECT id, name, format, last_modified FROM ckan_resource LIMIT 10;
```

Check outdated resources:
```sql
SELECT id, name, format, last_modified FROM ckan_resource WHERE id IN (SELECT resource_id FROM ckan_resource_last_update);
```

## Tips

- Run from the project root directory.
- Use `timeout_ms` of at least 60000 for queries, as the initial extension loading takes time.
- The first run is slower because DuckDB needs to install extensions. Subsequent runs are faster.
