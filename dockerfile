FROM python:3.11-slim

# 1. Environment settings
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    POETRY_VERSION=2.3.2 \
    POETRY_HOME="/opt/poetry"

# 2. Installation of dependencies and global Poetry
RUN apt-get update && apt-get upgrade -y && apt-get install -y \
    curl build-essential libpq-dev git gcc python3-dev \
    && curl -sSL https://install.python-poetry.org | python3 - \
    && /opt/poetry/bin/poetry self add "poetry-plugin-export" "poetry-audit-plugin" \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Add Poetry to global PATH
ENV PATH="$POETRY_HOME/bin:$PATH"

WORKDIR /app

# 3. Dependency Management
# Copy only dependency files first to leverage Docker cache
COPY pyproject.toml poetry.lock* /app/
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi --no-root

# Copy the rest of the application
COPY . /app

# 4. User and Permissions Adjustment
# Create the user and explicitly give permissions to the /app folder
RUN useradd -u 1000 -m analyst && chown -R analyst:analyst /app
USER analyst

CMD ["sleep", "infinity"]
