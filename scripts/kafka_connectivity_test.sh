#!/usr/bin/env bash
# Phase 1 requirement: a scripted connectivity test that produces a
# message to the real Kafka broker and consumes it back - proving Kafka
# itself is integrated, independent of Debezium/Spark (which have their
# own, separate proof: the 7 bluesky.public.* topics with real CDC data).
set -euo pipefail

cd "$(dirname "$0")/.."

TOPIC="_connectivity_test"
MESSAGE="ping-$(date +%s)"

echo "[kafka_connectivity_test] creating topic '$TOPIC' (if not exists)..."
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --if-not-exists \
    --topic "$TOPIC" --partitions 1 --replication-factor 1

echo "[kafka_connectivity_test] producing message: $MESSAGE"
echo "$MESSAGE" | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic "$TOPIC"

echo "[kafka_connectivity_test] consuming it back..."
RECEIVED=$(docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic "$TOPIC" \
    --from-beginning --max-messages 1 --timeout-ms 10000 2>/dev/null | tr -d '\r')

echo "[kafka_connectivity_test] received: $RECEIVED"

if [ "$RECEIVED" = "$MESSAGE" ]; then
    echo "[kafka_connectivity_test] PASS - message round-tripped through Kafka successfully"
    exit 0
else
    echo "[kafka_connectivity_test] FAIL - expected '$MESSAGE', got '$RECEIVED'"
    exit 1
fi
