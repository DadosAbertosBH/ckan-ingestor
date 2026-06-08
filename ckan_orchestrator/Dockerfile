FROM python:3.12-slim

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/bin/uv

# Copy and install ckan_ingestor first (resolves its deps: duckdb, pyarrow, minio, etc.)
COPY pyproject.toml README.md /app/ckan_ingestor-project/
COPY ckan_ingestor/ /app/ckan_ingestor-project/ckan_ingestor/

# Copy the orchestrator backend
COPY ckan_orchestrator/backend/ /app/backend/

# Install both: ckan_ingestor deps + orchestrator deps (which includes ckan-ingestor as local dep)
RUN uv pip install --system /app/ckan_ingestor-project/ && \
    uv pip install --system /app/backend/

EXPOSE 8000

CMD ["uvicorn", "ckan_orchestrator.main:app", "--host", "0.0.0.0", "--port", "8000"]
