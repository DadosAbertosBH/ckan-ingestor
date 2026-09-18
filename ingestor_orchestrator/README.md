# CKAN Orchestrator

Serviço composto para orquestração de ingestão de dados do portal CKAN. A API
FastAPI mantém as consultas; o binário Go é responsável por toda comunicação
com Apache Iggy, operações de escrita, scheduler e consumo de resultados.

## Arquitetura

```mermaid
graph TD
    A[Vue.js Frontend]:::accent0 -->|consultas| B[FastAPI API]:::accent1
    A -->|retry e sync :8081| C[Serviço Go]:::accent2
    C -->|Apache Iggy| D[Workers Rust]:::accent3
    D -->|resultados Iggy| C
    C -->|atualiza estado| G[MySQL]:::accent6
    B -->|consulta estado| G
```

| Componente | Tecnologia | Descrição |
|---|---|---|
| **API** | FastAPI | Endpoints REST de leitura e monitoramento |
| **Orquestração** | Go + Apache Iggy | Retry, sync de metadados, scheduler e consumo idempotente de resultados |
| **Worker** | Rust + Iggy | Consome mensagens da fila e executa a ingestão |
| **Frontend** | Vue.js 3 + TypeScript | Dashboard para visualizar e gerenciar jobs |
| **Iggy** | Apache Iggy | Fila de mensagens persistente para comunicação assíncrona |
| **MySQL** | 8.0 | Armazena estado dos jobs via SQLAlchemy async |

## Como rodar com Docker

```bash
# Copie o arquivo de environment
cp .env.example .env
# Edite o .env com suas configurações

# Suba os serviços
docker compose up -d

# Com frontend (perfil dev)
docker compose --profile dev up -d
```

A API estará disponível em `http://localhost:8000`, a API Go de escrita em
`http://localhost:8081`, e a documentação interativa em
`http://localhost:8000/docs`.

## Como rodar sem Docker

### Pré-requisitos

- Python 3.13+
- Go 1.26+
- MySQL 8.0
- Apache Iggy 0.8+

### Instalação

```bash
# Instalar uv (se necessário)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Instalar dependências do projeto base
cd /caminho/para/ckan-ingestor
uv pip install -e .

# Instalar o orchestrator
cd ingestor_orchestrator/backend
uv pip install -e .
```

### Executar

```bash
# API de leitura
uvicorn ingestor_orchestrator.main:app --host 0.0.0.0 --port 8000

# Serviço Go: Iggy, escrita, scheduler e resultados
cd ../go
go run ./cmd/orchestrator

# Frontend (dev)
cd ../frontend
npm install
npm run dev
```

## Endpoints da API

| Método | Path | Descrição |
|---|---|---|
| `GET` | `/health` | Health check |
| `GET` | `/api/dashboard/stats` | Estatísticas do dashboard |
| `GET` | `/api/jobs` | Lista jobs (filtros: `status`, `resource_id`, `limit`, `offset`) |
| `GET` | `/api/jobs/{id}` | Detalhe do job com resultados |
| `POST` | `http://localhost:8081/api/jobs/{id}/retry` | Retry de um job com falha |
| `POST` | `http://localhost:8081/api/metadata/sync[/{instance_id}]` | Dispara sincronização de metadados |
| `DELETE` | `/api/jobs/{id}` | Remove um job pending ou failed |

## Modelo de Dados

### CkanDataJob

| Campo | Tipo | Descrição |
|---|---|---|
| `id` | UUID | Identificador único do job |
| `resource_id` | string | ID do recurso CKAN |
| `resource_name` | string | Nome do recurso |
| `resource_url` | string | URL do recurso |
| `resource_format` | string | Formato (CSV, JSON, PDF, etc.) |
| `status` | enum | `pending` → `processing` → `completed` / `failed` |
| `idempotency_key` | string | Chave de idempotência (= resource_id) |
| `created_at` | datetime | Data de criação |
| `started_at` | datetime | Data de início do processamento |
| `completed_at` | datetime | Data de conclusão |

### CkanDataJobResult

| Campo | Tipo | Descrição |
|---|---|---|
| `id` | UUID | Identificador único |
| `job_id` | UUID (FK) | Referência ao job |
| `success` | boolean | Se a ingestão teve sucesso |
| `error_message` | text | Mensagem de erro (se falha) |
| `error_trace` | text | Stack trace completo (se falha) |
| `dataset_preview` | JSON | Preview das primeiras linhas (se sucesso) |
| `rows_processed` | integer | Número de linhas processadas |
| `created_at` | datetime | Data de criação do resultado |

## Idempotência

O coordinator Rust publica os jobs e o serviço Go os persiste a partir de
eventos `PENDING`:

1. O mesmo `job_id` é idempotente e mantém o registro existente.
2. Cada novo `job_id` cria seu próprio registro `pending`, inclusive quando
   houver outro job ativo para a mesma resource.
3. A ingestão em si usa `CREATE OR REPLACE TABLE` e merge/upsert no DuckLake.
4. Jobs `failed` podem ser retentados pelo endpoint de retry.

## Configuração

Todas as variáveis de ambiente usam o prefixo `INGEST_ORCH_`.

| Variável | Padrão | Descrição |
|---|---|---|
| `INGEST_ORCH_MYSQL_HOST` | `localhost` | Host do MySQL |
| `INGEST_ORCH_MYSQL_PORT` | `3306` | Porta do MySQL |
| `INGEST_ORCH_MYSQL_USER` | `root` | Usuário do MySQL |
| `INGEST_ORCH_MYSQL_PASSWORD` | (vazio) | Senha do MySQL |
| `INGEST_ORCH_MYSQL_DATABASE` | `ingestor_orchestrator` | Database do MySQL |
| `INGEST_ORCH_GO_ADDRESS` | `:8081` | Endereço HTTP do serviço Go |
| `INGEST_ORCH_IGGY_ADDRESS` | `localhost:8090` | Endpoint TCP do Apache Iggy |
| `INGEST_ORCH_IGGY_STREAM` | `ckan-ingestor` | Stream Iggy |
| `INGEST_ORCH_SCHEDULER_INTERVAL_MINUTES` | `480` | Intervalo do scheduler em minutos (padrão: 8h) |
| `INGEST_ORCH_DEBUG` | `false` | Modo debug |

Variáveis do `ckan_ingestor` (prefixo `DUCKLAKE_` e `S3_`) também são necessárias para o Worker e Scheduler.
