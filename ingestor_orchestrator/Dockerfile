# Orchestrator image — Go API, consumers, scheduler, migrations, and frontend

# --- Build frontend ---
FROM node:22-slim AS frontend-builder
WORKDIR /src
COPY ingestor_orchestrator/frontend/package.json ingestor_orchestrator/frontend/package-lock.json ./
RUN npm ci
COPY ingestor_orchestrator/frontend/ ./
RUN npx vite build

# --- Go API, Iggy consumers, and scheduler ---
FROM golang:1.26-bookworm AS go-builder
WORKDIR /src/ingestor_orchestrator
COPY ingestor_orchestrator/iggy-message-transport ./iggy-message-transport
COPY ingestor_orchestrator/ingestor-job-message-contracts ./ingestor-job-message-contracts
COPY ingestor_orchestrator/sicom-source-identity ./sicom-source-identity
COPY ingestor_orchestrator/backend/go.mod ingestor_orchestrator/backend/go.sum ./backend/
WORKDIR /src/ingestor_orchestrator/backend
RUN go mod download
COPY ingestor_orchestrator/backend/ ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/orchestrator-go ./cmd/orchestrator

# --- Minimal Go runtime ---
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=frontend-builder /src/dist /app/frontend
COPY --from=go-builder /out/orchestrator-go /usr/local/bin/orchestrator-go
EXPOSE 8000
ENTRYPOINT ["/usr/local/bin/orchestrator-go"]
