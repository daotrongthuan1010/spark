# Dùng OpenJDK 17 làm base image
FROM openjdk:17-jdk-slim

# Thiết lập thư mục làm việc
WORKDIR /app

# Copy file JAR của Spring Boot (giả sử đã build với Gradle)
COPY build/libs/spark-demo-*.jar app.jar

# Expose port (thay đổi nếu cần)
EXPOSE 8082

# Chạy ứng dụng
ENTRYPOINT ["java", "-jar", "app.jar"]