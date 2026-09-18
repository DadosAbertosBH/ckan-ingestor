# Orchestrator image — API, worker, scheduler, and frontend

# --- Build frontend ---
FROM node:22-slim AS frontend-builder
WORKDIR /src
COPY ingestor_orchestrator/frontend/package.json ingestor_orchestrator/frontend/package-lock.json ./
RUN npm ci
COPY ingestor_orchestrator/frontend/ ./
RUN npx vite build

# --- Go API, Iggy consumers, and scheduler ---
FROM golang:1.26-bookworm AS go-builder
WORKDIR /src
COPY ingestor_orchestrator/go/go.mod ingestor_orchestrator/go/go.sum ./
RUN go mod download
COPY ingestor_orchestrator/go/ ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/orchestrator-go ./cmd/orchestrator

# --- Python backend ---
FROM python:3.13-slim

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/bin/uv
COPY --from=frontend-builder /src/dist /app/frontend
COPY --from=go-builder /out/orchestrator-go /usr/local/bin/orchestrator-go

# Copy only dependency manifests first — layers stay cached when code changes
COPY ingestor_orchestrator/backend/pyproject.toml /app/ingestor_orchestrator/backend/

# Create a placeholder package so dependencies can be cached before source changes
RUN mkdir -p /app/ingestor_orchestrator/backend/ingestor_orchestrator && \
    touch /app/ingestor_orchestrator/backend/ingestor_orchestrator/__init__.py

# Install dependencies — this layer is cached as long as pyproject.toml doesn't change
RUN uv pip install --system /app/ingestor_orchestrator/backend/

# Now copy the actual source code (fast, only this layer is rebuilt on code changes)
COPY ingestor_orchestrator/backend/ /app/ingestor_orchestrator/backend/
COPY ingestor_orchestrator/entrypoint.sh /usr/local/bin/orchestrator-entrypoint
RUN chmod +x /usr/local/bin/orchestrator-entrypoint

# Ensure the real source is used, not the stale placeholder from pip install
ENV PYTHONPATH=/app/ingestor_orchestrator/backend

EXPOSE 8000 8081

ENTRYPOINT ["/usr/local/bin/orchestrator-entrypoint"]
CMD ["uvicorn", "ingestor_orchestrator.main:app", "--host", "0.0.0.0", "--port", "8000"]
