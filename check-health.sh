#!/bin/bash

echo "=== Checking System Health ==="

# Function to check HTTP endpoint
check_endpoint() {
    local url=$1
    local name=$2
    echo -n "Checking $name ($url): "
    
    if curl -s -f "$url" > /dev/null 2>&1; then
        echo "✅ UP"
    else
        echo "❌ DOWN"
    fi
}

# Function to check TCP port
check_port() {
    local host=$1
    local port=$2
    local name=$3
    echo -n "Checking $name ($host:$port): "
    
    if nc -z "$host" "$port" 2>/dev/null; then
        echo "✅ UP"
    else
        echo "❌ DOWN"
    fi
}

echo ""
echo "=== HTTP Services ==="
check_endpoint "http://localhost:8085/actuator/health" "Spring Boot"
check_endpoint "http://localhost:9090/-/healthy" "Prometheus"
check_endpoint "http://localhost:3000/api/health" "Grafana"
check_endpoint "http://localhost:8082" "Spark Master"
check_endpoint "http://localhost:8081" "Spark Worker"
check_endpoint "http://localhost:9187/metrics" "PostgreSQL Exporter"
check_endpoint "http://localhost:9308/metrics" "Kafka Exporter"

echo ""
echo "=== TCP Services ==="
check_port "localhost" "5432" "PostgreSQL"
check_port "localhost" "9092" "Kafka"
check_port "localhost" "2181" "Zookeeper"
check_port "localhost" "7077" "Spark Master Port"

echo ""
echo "=== Prometheus Targets ==="
echo "Checking Prometheus targets..."
curl -s "http://localhost:9090/api/v1/targets" | jq -r '.data.activeTargets[] | "\(.labels.job): \(.health)"' 2>/dev/null || echo "jq not installed, check manually at http://localhost:9090/targets"

echo ""
echo "=== Docker Containers ==="
docker-compose ps

echo ""
echo "=== System Check Complete ==="
