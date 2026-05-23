# Stage 1: Build the fat JAR with Maven
FROM maven:3.9-eclipse-temurin-25 AS builder

# Set the working directory inside the container
WORKDIR /app

# Copy the POM first for layer-cache-friendly dependency resolution
COPY pom.xml ./
RUN mvn dependency:go-offline -q

# Copy the full source tree and build (skip tests for a faster image build)
COPY src ./src
RUN mvn package -DskipTests -q

# Stage 2: Minimal JRE runtime image
FROM eclipse-temurin:25-jre-alpine

# Install tzdata and ca-certificates (matches the Go Dockerfile)
RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

# Copy only the executable fat JAR from the builder stage
COPY --from=builder /app/target/bitcask-1.0.0.jar ./bitcask-server.jar

# Set default environment variables (mirrors the Go Dockerfile)
ENV PORT=9092
ENV BITCASK_DIR=/bitcask/data

# Expose the default server port
EXPOSE $PORT

# Create the data directory
RUN mkdir -p /bitcask/data

# Run the Spring Boot JAR
ENTRYPOINT ["java", "-jar", "./bitcask-server.jar"]
