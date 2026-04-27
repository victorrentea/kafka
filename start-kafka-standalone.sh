#!/usr/bin/env bash
set -e

KAFKA_VERSION="3.8.1"
SCALA_VERSION="2.13"
KAFKA_DIR="$HOME/workspace/kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
TARBALL="$HOME/workspace/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
ARCHIVE_URL="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
LOG_FILE="/tmp/kafka.log"

# Download if not present
if [ ! -f "$TARBALL" ]; then
  echo "Downloading Kafka ${KAFKA_VERSION}..."
  curl -o "$TARBALL" "$ARCHIVE_URL"
fi

# Extract if not present
if [ ! -d "$KAFKA_DIR" ]; then
  echo "Extracting..."
  tar -xzf "$TARBALL" -C "$HOME/workspace"
fi

# Format storage if not already done
if [ ! -f "/tmp/kraft-combined-logs/meta.properties" ]; then
  echo "Formatting KRaft storage..."
  KAFKA_CLUSTER_ID=$("$KAFKA_DIR/bin/kafka-storage.sh" random-uuid)
  "$KAFKA_DIR/bin/kafka-storage.sh" format -t "$KAFKA_CLUSTER_ID" -c "$KAFKA_DIR/config/kraft/server.properties"
fi

# Start Kafka
echo "Starting Kafka (logs: $LOG_FILE)..."
"$KAFKA_DIR/bin/kafka-server-start.sh" "$KAFKA_DIR/config/kraft/server.properties" &> "$LOG_FILE" &
echo "Kafka PID: $!"

# Wait and verify
sleep 5
if grep -q "Kafka Server started" "$LOG_FILE"; then
  echo "Kafka is up on localhost:9092"
else
  echo "Kafka may have failed to start — check $LOG_FILE"
  grep "ERROR" "$LOG_FILE" | tail -5
fi
