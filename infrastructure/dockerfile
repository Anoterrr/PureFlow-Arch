FROM python:3.11-slim

# Instala dependências de sistema
RUN apt-get update && apt-get install -y \
    curl build-essential libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Instala Poetry
ENV POETRY_VERSION=1.7.1
RUN curl -sSL https://install.python-poetry.org | python3 -
ENV PATH="/root/.local/bin:$PATH"

WORKDIR /app
COPY pyproject.toml poetry.lock* /app/

# Configura poetry para não criar venv dentro do container
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi

COPY . /app