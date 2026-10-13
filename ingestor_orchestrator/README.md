# CKAN Orchestrator

Aplicação Go responsável pela API HTTP, frontend Vue, comunicação com Apache
Iggy, scheduler, consumo de resultados e persistência no MySQL.

## Arquitetura

```mermaid
graph LR
    A[Browser] -->|HTTP :8000| B[Go API + frontend]
    B --> C[MySQL]
    B --> D[Apache Iggy]
    D --> E[Workers Rust]
    E -->|resultados| D
```

O frontend usa paths relativos `/api`; HTTP e HTTPS são terminados pelo mesmo
origin, incluindo ambientes expostos pelo OrbStack. A aplicação publica apenas
a porta `8000`.

O `source-sync-worker` descobre CKAN e SICOM e persiste os recursos no catálogo.
Consulta o histórico de jobs da API para selecionar versões ainda não concluídas
e publica diretamente no Iggy, com IDs estáveis. A API persiste os jobs ao consumir
as mensagens. O resultado de sucesso do sync é publicado depois da confirmação
de todas as mensagens de jobs; só então o consumidor confirma o comando de sync.
Uma falha de publicação mantém o comando disponível para reentrega e republicação
idempotente, incluindo os anúncios de recursos removidos.

O catálogo identifica cada recurso por `(source_id, id)`. No CKAN, `id` é o
identificador original do package/resource; no SICOM, o adapter fornece uma
identidade determinística. O catálogo persiste apenas os campos normalizados,
compara esses campos para contabilizar alterações e calcula os IDs de jobs a
partir de fonte, recurso e versão durante a publicação.

A migration 7 foi atualizada antes da publicação. Bases locais que já aplicaram
a definição anterior precisam de adequação do catálogo antes de executar esta
versão; o Goose considera essa migration já aplicada e preserva o schema antigo.

O fluxo usa `ckan_data_job` como histórico de processamento. As tabelas legadas
`source_sync_outbox`, `source_resource_version` e `source_sync_run`, criadas pela
migration 7, ficam preservadas em bancos existentes e sem uso pelo novo fluxo.

## Docker

```bash
cp ingestor_orchestrator/.env.example ingestor_orchestrator/.env
docker compose -f ingestor_orchestrator/docker-compose.yml build --pull source-sync-worker
docker compose -f ingestor_orchestrator/docker-compose.yml up -d
```

O Dockerfile do `source-sync-worker` copia o executável diretamente da imagem
oficial `lightpanda/browser:nightly` e o inicia dentro do próprio container
durante o sync do SICOM. Para atualizar o nightly, execute novamente o comando
de build com `--pull`.

- Aplicação: `http://localhost:8000`
- OpenAPI: `http://localhost:8000/openapi.json`
- Swagger UI: `http://localhost:8000/docs`
- Health: `http://localhost:8000/health`
- Readiness: `http://localhost:8000/ready`

O serviço `db-init` executa `orchestrator-go migrate`. As migrations SQL ficam
embutidas no binário e aceitam tanto um database vazio quanto um database
legado no head Alembic `020`.

## Desenvolvimento

Pré-requisitos: Go 1.26+, Node.js 22+, MySQL 8.0 e Apache Iggy 0.8.

```bash
cd ingestor_orchestrator/backend
go run ./cmd/orchestrator migrate
go run ./cmd/orchestrator

cd ../frontend
npm ci
npm run dev
```

## API

| Método | Path | Descrição |
|---|---|---|
| `GET` | `/api/instances/` | Lista instâncias CKAN |
| `POST` | `/api/instances/` | Cria uma instância |
| `GET`, `DELETE` | `/api/instances/{id}` | Consulta ou remove uma instância |
| `GET` | `/api/jobs/` | Lista jobs com filtros e paginação |
| `GET` | `/api/jobs/{id}` | Detalhe e resultados do job |
| `POST` | `/api/jobs/{id}/retry` | Reenvia um job com falha |
| `GET` | `/api/resources/` | Lista recursos agregados |
| `GET` | `/api/resources/{id}` | Detalhe, histórico e preview |
| `GET` | `/api/datasets/` | Lista datasets agregados |
| `GET` | `/api/dashboard/stats` | Estatísticas por instância |
| `GET` | `/api/syncs/`, `/api/syncs/{id}` | Histórico de sincronizações |
| `POST` | `/api/metadata/sync[/{instance_id}]` | Dispara sincronização |

## Configuração principal

| Variável | Padrão | Descrição |
|---|---|---|
| `INGEST_ORCH_GO_ADDRESS` | `:8000` | Endereço HTTP |
| `INGEST_ORCH_STATIC_DIR` | `/app/frontend` | Assets compilados do frontend |
| `INGEST_ORCH_MYSQL_HOST` | `localhost` | Host MySQL |
| `INGEST_ORCH_MYSQL_PORT` | `3306` | Porta MySQL |
| `INGEST_ORCH_MYSQL_USER` | `root` | Usuário MySQL |
| `INGEST_ORCH_MYSQL_PASSWORD` | vazio | Senha MySQL |
| `INGEST_ORCH_MYSQL_DATABASE` | `ingestor_orchestrator` | Database MySQL |
| `INGEST_ORCH_IGGY_ADDRESS` | `localhost:8090` | Endpoint TCP do Iggy |
| `INGEST_ORCH_IGGY_STREAM` | `ckan-ingestor` | Stream Iggy |
| `INGEST_ORCH_SCHEDULER_INTERVAL_MINUTES` | `480` | Intervalo do scheduler |
