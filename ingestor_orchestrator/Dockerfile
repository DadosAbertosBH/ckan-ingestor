# Orchestrator image — API, worker, scheduler, and frontend
FROM python:3.13-slim

WORKDIR /app

# --- Build frontend ---
FROM node:22-slim AS frontend-builder
WORKDIR /src
COPY ingestor_orchestrator/frontend/package.json ingestor_orchestrator/frontend/package-lock.json ./
RUN npm ci
COPY ingestor_orchestrator/frontend/ ./
RUN npm run build

# --- Python backend ---
FROM python:3.13-slim

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/bin/uv
COPY --from=frontend-builder /src/dist /app/frontend

# Copy only dependency manifests first — layers stay cached when code changes
COPY pyproject.toml README.md /app/
COPY ingestor_orchestrator/backend/pyproject.toml /app/ingestor_orchestrator/backend/

# Create placeholder packages so `uv pip install` can resolve local deps without source
RUN mkdir -p /app/ckan_ingestor && \
    touch /app/ckan_ingestor/__init__.py && \
    mkdir -p /app/ingestor_orchestrator/backend/ingestor_orchestrator && \
    touch /app/ingestor_orchestrator/backend/ingestor_orchestrator/__init__.py

# Install dependencies — this layer is cached as long as pyproject.toml doesn't change
RUN uv pip install --system /app/ && \
    uv pip install --system /app/ingestor_orchestrator/backend/

# Now copy the actual source code (fast, only this layer is rebuilt on code changes)
COPY ckan_ingestor/ /app/ckan_ingestor/
COPY ingestor_orchestrator/backend/ /app/ingestor_orchestrator/backend/

# Ensure the real source is used, not the stale placeholder from pip install
ENV PYTHONPATH=/app/ingestor_orchestrator/backend:/app

EXPOSE 8000

CMD ["uvicorn", "ingestor_orchestrator.main:app", "--host", "0.0.0.0", "--port", "8000"]
