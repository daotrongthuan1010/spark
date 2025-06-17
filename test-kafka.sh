#!/bin/bash

echo "=== Testing Kafka Connectivity ==="

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
timeout 60 bash -c 'until docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list; do sleep 2; done'

# Create topic if not exists
echo "Creating sales-topic..."
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic sales-topic --partitions 3 --replication-factor 1 --if-not-exists

# List topics
echo "Listing topics..."
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Test producer
echo "Testing producer..."
echo "test message" | docker-compose exec -T kafka kafka-console-producer --bootstrap-server localhost:9092 --topic sales-topic

# Test consumer (read last message)
echo "Testing consumer..."
timeout 5 docker-compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic sales-topic --from-beginning --max-messages 1

echo "=== Kafka test complete ==="
