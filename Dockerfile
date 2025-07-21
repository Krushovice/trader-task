FROM python:3.13-slim

# Устанавливаем Poetry
RUN pip install --no-cache-dir poetry

WORKDIR /app

# Копируем описания зависимостей
COPY pyproject.toml poetry.lock* ./

# Устанавливаем зависимости без создания виртуального окружения
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi

# Копируем исходники
COPY src/ ./src/

# Запускаем main через Poetry
CMD ["poetry", "run", "python", "-u", "src/main.py"]