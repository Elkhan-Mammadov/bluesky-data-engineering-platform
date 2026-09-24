# Simulator service: generates Jetstream-shaped synthetic events and serves
# server.py, used as a fallback when SOURCE_MODE=simulator.
FROM python:3.12.5-slim-bookworm

WORKDIR /app

COPY requirements/simulator.txt requirements/simulator.txt
RUN pip install --no-cache-dir -r requirements/simulator.txt

COPY ingestion ingestion
COPY config config

ENV PYTHONPATH=/app
EXPOSE 8001

HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=5 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8001/health', timeout=2)" || exit 1

CMD ["uvicorn", "ingestion.simulator.server:app", "--host", "0.0.0.0", "--port", "8001"]
