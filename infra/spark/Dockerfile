# Shared Spark image for spark-master, spark-worker and spark-streaming.
# Which process runs is decided by the `command:` set per service in
# docker-compose.yml, not by this Dockerfile.
FROM apache/spark:3.5.1-python3

USER root
WORKDIR /opt/app

COPY requirements/spark.txt requirements/spark.txt
RUN pip install --no-cache-dir -r requirements/spark.txt

COPY streaming streaming
COPY config config

ENV PYTHONPATH=/opt/app
