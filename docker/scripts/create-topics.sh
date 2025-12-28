#!/bin/bash
set -e

echo "Waiting for Kafka broker socket..."

until nc -z kafka 9092; do
  sleep 1
done

echo "Kafka socket is open. Waiting for controller..."

sleep 10

echo "Creating topics..."

/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create \
  --topic binance_trades \
  --partitions 6 \
  --replication-factor 1

echo "Topic setup complete."
