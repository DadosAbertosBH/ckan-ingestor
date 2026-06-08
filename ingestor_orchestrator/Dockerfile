FROM python:3.12-slim

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/bin/uv

# Copy only dependency manifests first — layers stay cached when code changes
COPY pyproject.toml README.md /app/ckan_ingestor-project/
COPY ingestor_orchestrator/backend/pyproject.toml /app/backend/

# Create placeholder packages so `uv pip install` can resolve local deps without source
RUN mkdir -p /app/ckan_ingestor-project/ckan_ingestor && \
    touch /app/ckan_ingestor-project/ckan_ingestor/__init__.py && \
    mkdir -p /app/backend/ckan_orchestrator && \
    touch /app/backend/ckan_orchestrator/__init__.py && \
    mkdir -p /app/backend/alembic && \
    touch /app/backend/alembic/__init__.py

# Install dependencies — this layer is cached as long as pyproject.toml doesn't change
RUN uv pip install --system /app/ckan_ingestor-project/ && \
    uv pip install --system /app/backend/

# Now copy the actual source code (fast, only this layer is rebuilt on code changes)
COPY ckan_ingestor/ /app/ckan_ingestor-project/ckan_ingestor/
COPY ingestor_orchestrator/backend/ /app/backend/

# Ensure the real source is used, not the stale placeholder from pip install
ENV PYTHONPATH=/app/backend:/app/ckan_ingestor-project

EXPOSE 8000

CMD ["uvicorn", "ckan_orchestrator.main:app", "--host", "0.0.0.0", "--port", "8000"]
