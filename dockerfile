# Use a slim Python image for efficiency
FROM python:3.11-slim

# 1. Dependências de sistema (essenciais para DuckDB e dbt)
RUN apt-get update && apt-get install -y \
    curl build-essential libpq-dev git gcc python3-dev \
    && rm -rf /var/lib/apt/lists/*

# 2. Instala Poetry 2.0+ (suporte nativo ao bloco [project])
ENV POETRY_VERSION=2.0.1
RUN curl -sSL https://install.python-poetry.org | python3 -
ENV PATH="/root/.local/bin:$PATH"

WORKDIR /app

# 3. Copia apenas os arquivos de dependência primeiro
COPY pyproject.toml poetry.lock* /app/

# 4. Instalação sem ambiente virtual (dentro do container)
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi --no-root

COPY . /app

CMD ["tail", "-f", "/dev/null"]