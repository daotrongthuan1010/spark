# Spark Demo Project

Dự án demo tích hợp Spring Boot, Apache Spark, Kafka, PostgreSQL với monitoring bằng Prometheus và Grafana.

## Kiến trúc hệ thống

- **Spring Boot**: API và business logic
- **Apache Spark**: Xử lý dữ liệu real-time từ Kafka
- **Apache Kafka**: Message streaming
- **PostgreSQL**: Database chính
- **Prometheus**: Monitoring và metrics
- **Grafana**: Dashboard và visualization

## Cách chạy hệ thống

### 1. Chạy toàn bộ hệ thống với Docker

```bash
# Trên Linux/Mac
chmod +x start-system.sh
./start-system.sh

# Trên Windows
start-system.bat
```

### 2. Chạy từng bước

```bash
# Build ứng dụng Spring Boot
./gradlew build -x test

# Khởi động tất cả services
docker-compose up -d

# Kiểm tra trạng thái
docker-compose ps
```

## Các service và ports

| Service | URL | Port | Mô tả |
|---------|-----|------|-------|
| Spring Boot | http://localhost:8085 | 8085 | API chính |
| Prometheus | http://localhost:9090 | 9090 | Monitoring |
| Grafana | http://localhost:3000 | 3000 | Dashboard (admin/admin) |
| Spark Master | http://localhost:8082 | 8082 | Spark Web UI |
| Spark Worker | http://localhost:8081 | 8081 | Worker Web UI |
| PostgreSQL | localhost:5432 | 5432 | Database |
| Kafka | localhost:9092 | 9092 | Message broker |

## Test hệ thống

```bash
# Test API endpoint
curl http://localhost:8085/process-spark

# Kiểm tra metrics
curl http://localhost:8085/actuator/prometheus

# Kiểm tra health
curl http://localhost:8085/actuator/health
```

## Giải quyết các vấn đề thường gặp

### 1. Prometheus không hiển thị service UP
- Kiểm tra network connectivity giữa các container
- Verify endpoints trong prometheus.yml
- Kiểm tra health endpoints của các service

### 2. Kafka connection error
- Đảm bảo Kafka đã khởi động hoàn toàn
- Kiểm tra KAFKA_ADVERTISED_LISTENERS configuration
- Verify network connectivity

### 3. Spring Boot không connect được Kafka
- Kiểm tra bootstrap-servers configuration
- Đảm bảo topic đã được tạo
- Verify Kafka exporter metrics

## Monitoring

### Prometheus targets
- spring-app:8085/actuator/prometheus
- spark-master:8080/metrics/prometheus  
- spark-worker:8081/metrics/prometheus
- postgres-exporter:9187
- kafka-exporter:9308

### Grafana dashboards
- Import dashboard cho Spring Boot metrics
- Import dashboard cho Kafka metrics
- Import dashboard cho PostgreSQL metrics

## Logs

```bash
# Xem logs của tất cả services
docker-compose logs -f

# Xem logs của service cụ thể
docker-compose logs -f spring-app
docker-compose logs -f kafka
docker-compose logs -f prometheus
```

## Dừng hệ thống

```bash
docker-compose down
```
