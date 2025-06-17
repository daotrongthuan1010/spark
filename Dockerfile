# Dùng OpenJDK 17 làm base image
FROM openjdk:17-jdk-slim

# Thiết lập thư mục làm việc
WORKDIR /app

# Copy Gradle wrapper và build files
COPY gradle gradle
COPY gradlew .
COPY gradlew.bat .
COPY build.gradle .
COPY settings.gradle .

# Copy source code
COPY src src

# Build ứng dụng
RUN chmod +x ./gradlew && ./gradlew build -x test

# Expose port
EXPOSE 8085

# Chạy ứng dụng với profile docker
ENTRYPOINT ["java", "-jar", "-Dspring.profiles.active=docker", "build/libs/spark-demo-0.0.1-SNAPSHOT.jar"]