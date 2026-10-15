# Database repositories

`repository-contracts` contains the interfaces, shared read models and domain
errors. `mysql-repositories` owns MySQL persistence and its existing seven Goose
migrations. `oracle-repositories` owns Oracle persistence and restartable schema
initialization. Implementations import contracts rather than backend internals.
The API and `source-resource-catalog` retain type aliases for their callers.

Both API and source sync worker select `INGEST_ORCH_DB_PROVIDER` (`oracle` by
default, or `mysql`). Oracle requires `INGEST_ORCH_ORACLE_DSN`; existing MySQL
environment variables continue to configure that provider. Select the same
provider for API, migrations and source sync worker so instances, catalog and job
completion state are kept in one database.

## Local Docker

```sh
docker compose -f ingestor_orchestrator/docker-compose.yml up --build
```

The default stack uses Oracle Free with `FREEPDB1` over TCP on port 1521
(published to the host as 1522). The bootstrap creates the `CKAN_APP`
application user and the API migration command initializes the schema. The local
DSN is `oracle://CKAN_APP:ContractApp123@oracle:1521/FREEPDB1`.
The local Oracle database is ephemeral; recreating its container starts with an
empty database. The bootstrap waits for the PDB service to be registered after
the listener healthcheck passes.

Use `INGEST_ORCH_ORACLE_ADMIN_PASSWORD`, `INGEST_ORCH_ORACLE_PASSWORD` and
`INGEST_ORCH_ORACLE_DSN` to override the local development credentials. When
overriding the application password, provide a matching URL-encoded DSN. The
bootstrap preserves an existing application user and its password.

An Autonomous Database remains supported by supplying its own DSN and wallet
through the deployment configuration; the local Compose stack does not require
a wallet or TLS.

Oracle starts with a 2 GiB memory ceiling, configurable with
`INGEST_ORCH_ORACLE_MEMORY`. A lower tested ceiling should be selected from the
sizing experiment; resource requirements include database startup as well as
the application workload.

To use MySQL:

```sh
docker compose -f ingestor_orchestrator/docker-compose.yml \
  -f ingestor_orchestrator/docker-compose.mysql.yml up --build
```

The override selects MySQL consistently and activates its service. Existing
MySQL volumes and data are preserved. Oracle initializes a fresh schema; transfer
of existing MySQL history is a separate operation.

## Testing

Each implementation runs the same `repository-contracts/contracttest` integration
assertions. Set `MYSQL_INTEGRATION_DSN` or `ORACLE_INTEGRATION_DSN` for a dedicated
test schema, then run `go test ./...` in the corresponding implementation module.
The suite writes fixtures to that test schema. Oracle initialization retains all
4,064 CSV delimiter seeds and the default CKAN/SICOM sources.

The isolated Autonomous sizing experiment starts under 256 MiB, then tries
512 MiB, 1, 2, 4, 6 and 8 GiB, with four CPUs and swap disabled:

```sh
cd ingestor_orchestrator/oracle-repositories
ORACLE_DOCKER_TEST=1 go test -run TestOracleDockerSizing -v -count=1 -timeout 3h
```

Containers and their disposable volumes are removed after each attempt. Startup
has a 20-minute budget; OOM and startup timeouts advance the memory ceiling.
Schema, TLS and contract failures stop the experiment for investigation. A viable
case must complete the contract suite and insertion/replays of both 20,448 and
200,000 resources within 120 seconds per phase. `ORACLE_TEST_MEMORY` selects one
ceiling; `ORACLE_TEST_IMAGE` overrides the image for registry/digest testing.

The test reports startup failures and per-phase duration and Docker CPU/memory
stats. A candidate ceiling needs a second successful run before being adopted
as the Compose default. The current 2 GiB default is the documented reference
allocation, rather than a measured minimum.

## Deployment

Helm exposes `database.provider`, `oracle.dsn`, `oracle.walletSecretName` and
`oracle.walletMountPath`. Provide the DSN through the generated or existing
Secret, and the wallet through a separate Secret when mTLS is used. The wallet
is mounted into migrations, API and source sync worker. Database provisioning
remains external to the chart. GitOps activation and data transfer are separate
from this local implementation.
