FROM python:3.11-slim


RUN apt-get update && apt-get install -y \
    curl build-essential libpq-dev git gcc python3-dev \
    && rm -rf /var/lib/apt/lists/*


ENV POETRY_VERSION=2.0.1
RUN curl -sSL https://install.python-poetry.org | python3 -
ENV PATH="/root/.local/bin:$PATH"

WORKDIR /app
COPY pyproject.toml poetry.lock* /app/

RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi --no-root

COPY . /app

RUN useradd -m analyst && chown -R analyst:analyst /app
USER analyst

CMD ["sleep", "infinity"]