"""Entry point for the Spark Structured Streaming job.

Reads Debezium CDC messages from Kafka, cleans them, upserts into the
`raw` warehouse schema and computes `realtime` aggregates, with a 5-second
trigger. Full implementation lands in Stage 5 (Spark streaming).
"""

from __future__ import annotations


def build_spark_session():
    """TODO (Stage 5): create the SparkSession with the Postgres JDBC driver
    and Kafka package configured."""
    raise NotImplementedError("Implemented in Stage 5 (Spark streaming)")


def run() -> None:
    """TODO (Stage 5): wire together debezium_parser, cleaning and
    realtime_aggregates into one streaming query with a 5-second trigger."""
    raise NotImplementedError("Implemented in Stage 5 (Spark streaming)")


if __name__ == "__main__":
    run()
