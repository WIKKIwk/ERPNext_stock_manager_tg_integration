# syntax=docker/dockerfile:1

FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md /app/
COPY stock_manager_bot /app/stock_manager_bot

RUN pip install --upgrade pip && \
    pip install .

ENTRYPOINT ["python", "-m", "stock_manager_bot"]
