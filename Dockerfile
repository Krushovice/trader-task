FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    POETRY_VIRTUALENVS_CREATE=false

# базовые системные пакеты (для numpy/pandas колёс обычно не нужны компиляторы,
# но держим минимальный набор на всякий случай)
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential gcc \
 && rm -rf /var/lib/apt/lists/*

# Poetry
RUN pip install --no-cache-dir poetry

WORKDIR /app

# зависимости
COPY pyproject.toml poetry.lock* ./
RUN poetry install --no-interaction --no-ansi --only main

# исходники
COPY src/ ./src/

# по желанию: каталог для лок-файлов/логов
RUN mkdir -p /app/data
