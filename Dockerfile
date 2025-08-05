FROM python:3.11-slim

RUN apt-get update && apt-get install -y build-essential gcc && rm -rf /var/lib/apt/lists/*

# Устанавливаем Poetry
RUN pip install --no-cache-dir poetry

WORKDIR /app

# Копируем описание зависимостей
COPY pyproject.toml poetry.lock* ./

# Устанавливаем зависимости (без виртуального окружения)
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi

# Копируем исходники
COPY src/ ./src/

# Запускаем main через Poetry
CMD ["poetry", "run", "python", "-u", "src/main.py"]