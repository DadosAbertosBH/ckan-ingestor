FROM python:3.12-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ADD . .

RUN uv sync --locked

RUN uv run ckan_ingestor/run_duckdb_ingestor.py
