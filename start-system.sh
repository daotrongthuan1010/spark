#!/bin/bash

echo "=== Starting Spark Demo System ==="

# Build Spring Boot application
echo "Building Spring Boot application..."
./gradlew build -x test

# Start all services with Docker Compose
echo "Starting all services with Docker Compose..."
docker-compose up -d

# Wait for services to start
echo "Waiting for services to start..."
sleep 30

# Check service status
echo "Checking service status..."
docker-compose ps

echo "=== System started successfully! ==="
echo ""
echo "Services available at:"
echo "- Spring Boot App: http://localhost:8085"
echo "- Prometheus: http://localhost:9090"
echo "- Grafana: http://localhost:3000 (admin/admin)"
echo "- Spark Master UI: http://localhost:8082"
echo "- Spark Worker UI: http://localhost:8081"
echo "- PostgreSQL: localhost:5432"
echo "- Kafka: localhost:9092"
echo ""
echo "To test the system:"
echo "curl http://localhost:8085/process-spark"
