# syntax=docker/dockerfile:1

FROM golang:1.26-bookworm AS builder
WORKDIR /src/ingestor_orchestrator
COPY ingestor_orchestrator/iggy-message-transport ./iggy-message-transport
COPY ingestor_orchestrator/ingestor-job-message-contracts ./ingestor-job-message-contracts
COPY ingestor_orchestrator/sicom-source-identity ./sicom-source-identity
COPY ingestor_orchestrator/repository-contracts ./repository-contracts
COPY ingestor_orchestrator/mysql-repositories ./mysql-repositories
COPY ingestor_orchestrator/oracle-repositories ./oracle-repositories
COPY ingestor_orchestrator/source-resource-catalog ./source-resource-catalog
COPY ingestor_orchestrator/source-sync-message-contracts ./source-sync-message-contracts
COPY ingestor_orchestrator/ckan-source-discovery ./ckan-source-discovery
COPY ingestor_orchestrator/sicom-crawler ./sicom-crawler
COPY ingestor_orchestrator/source-sync-worker ./source-sync-worker
WORKDIR /src/ingestor_orchestrator/source-sync-worker
RUN go mod download && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/source-sync-worker ./cmd/source-sync-worker

FROM lightpanda/browser:nightly AS lightpanda

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates tini && rm -rf /var/lib/apt/lists/*
COPY --from=lightpanda /bin/lightpanda /usr/local/bin/lightpanda
COPY --from=builder /out/source-sync-worker /usr/local/bin/source-sync-worker
ENV LIGHTPANDA_DISABLE_TELEMETRY=true LIGHTPANDA_DISABLE_CORE_DUMP=true XDG_CACHE_HOME=/tmp XDG_DATA_HOME=/tmp TINI_SUBREAPER=true
USER 10001
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/source-sync-worker"]
