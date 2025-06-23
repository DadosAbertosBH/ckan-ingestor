FROM python:3.12-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

ADD . .

RUN uv venv /dagster_proj/.venv
ENV PATH="/dagster_proj/.venv/bin:$PATH"
RUN uv pip install --system -e .

EXPOSE 80
