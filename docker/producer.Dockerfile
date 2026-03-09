FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml .
RUN pip install --no-cache-dir ".[requests,kafka,gtfs]"

COPY ./src/shared ./src/shared
COPY ./src/apps/producer ./src/apps/producer

RUN useradd -m myuser && chown -R myuser /app

RUN pip install -e .

USER myuser

CMD ["python", "./src/apps/producer/mta_producer.py"]