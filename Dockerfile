FROM ubuntu:24.04

RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    docker.io \
 && update-ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /mlops-hw3-e2e-ml-pipeline

COPY pyproject.toml .
COPY uv.lock .

RUN uv sync --locked

ENV PATH="/mlops-hw3-e2e-ml-pipeline/.venv/bin:$PATH"

COPY scripts scripts/
COPY pipeline pipeline/
COPY dags dags/

RUN chmod +x scripts/*.sh
