# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM golang:1.26-bookworm AS go-builder
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src/ingestor_orchestrator
COPY ingestor_orchestrator/iggy-message-transport ./iggy-message-transport
COPY ingestor_orchestrator/ingestor-job-message-contracts ./ingestor-job-message-contracts
COPY ingestor_orchestrator/sicom-source-identity ./sicom-source-identity
COPY ingestor_orchestrator/source-resource-catalog ./source-resource-catalog
COPY ingestor_orchestrator/repository-contracts ./repository-contracts
COPY ingestor_orchestrator/sicom-crawler/go.mod ingestor_orchestrator/sicom-crawler/go.sum ./sicom-crawler/
WORKDIR /src/ingestor_orchestrator/sicom-crawler
RUN go mod download
COPY ingestor_orchestrator/sicom-crawler ./
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/sicom-crawler ./cmd/sicom-crawler

FROM lightpanda/browser:nightly AS lightpanda

FROM debian:trixie-slim
COPY --from=go-builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=go-builder /out/sicom-crawler /usr/local/bin/sicom-crawler
COPY --from=lightpanda /bin/lightpanda /usr/local/bin/lightpanda
ENV LIGHTPANDA_DISABLE_TELEMETRY=true LIGHTPANDA_DISABLE_CORE_DUMP=true XDG_CACHE_HOME=/tmp XDG_DATA_HOME=/tmp
USER 10001
ENTRYPOINT ["/usr/local/bin/sicom-crawler"]
