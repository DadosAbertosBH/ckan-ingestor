FROM registry.gitlab.com/pedalin/dagster-celery-k8s:4ecbc1ce
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

ADD . .

RUN uv venv /dagster_proj/.venv
ENV PATH="/dagster_proj/.venv/bin:$PATH"
RUN uv pip install --system -e .

EXPOSE 80
