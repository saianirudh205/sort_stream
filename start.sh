#!/bin/bash
set -e

echo "Starting Kafka..."

# Start Zookeeper
/opt/kafka/bin/zookeeper-server-start.sh -daemon /opt/kafka/config/zookeeper.properties
sleep 5

# Start Kafka
/opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/server.properties
sleep 5

# Create topics
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic source --partitions 4 --replication-factor 1

for t in id name continent; do
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic $t --partitions 1 --replication-factor 1
done

echo "Running source generator..."
./source

echo "Running sorter..."
./sort

echo "Done."
