# Use a slim Python image for efficiency
FROM python:3.11-slim

# 1. Install system dependencies (needed for DuckDB extensions and C-based libs)
RUN apt-get update && apt-get install -y \
    curl \
    build-essential \
    libpq-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

# 2. Install Poetry
ENV POETRY_VERSION=1.7.1
RUN curl -sSL https://install.python-poetry.org | python3 -
ENV PATH="/root/.local/bin:$PATH"

# 3. Set working directory
WORKDIR /app

# 4. Copy dependency files first (optimizes Docker layer caching)
COPY pyproject.toml poetry.lock* /app/

# 5. Install dependencies
# We tell Poetry not to create a virtualenv inside the container 
# because the container itself is already an isolated environment.
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi

# 6. Copy the rest of the application
COPY . /app

# Default command: keep container alive for interactive use
CMD ["tail", "-f", "/dev/null"]