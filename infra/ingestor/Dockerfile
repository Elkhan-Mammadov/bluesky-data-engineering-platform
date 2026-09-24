# Ingestor service: reads Bluesky Jetstream (or the simulator fallback),
# hashes/samples users, writes to source-db, and serves status_api.py.
FROM python:3.12.5-slim-bookworm

WORKDIR /app

COPY requirements/ingestor.txt requirements/ingestor.txt
RUN pip install --no-cache-dir -r requirements/ingestor.txt

COPY ingestion ingestion
COPY config config

ENV PYTHONPATH=/app
EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=5 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health', timeout=2)" || exit 1

CMD ["uvicorn", "ingestion.jetstream.status_api:app", "--host", "0.0.0.0", "--port", "8000"]
