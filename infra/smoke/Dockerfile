# One-shot container for `make smoke`. Runs the smoke test inside the
# compose network, so the host needs no Python packages at all.
FROM python:3.12-slim

COPY requirements/smoke.txt /tmp/requirements-smoke.txt
RUN pip install --no-cache-dir -r /tmp/requirements-smoke.txt

WORKDIR /app
