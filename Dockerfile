# Multi-stage Dockerfile for Consumer Application
# Stage 1: Build
FROM gradle:8.5-jdk17 AS builder

WORKDIR /build

# Copy gradle files first for better caching
COPY build.gradle settings.gradle* gradlew* ./
COPY gradle gradle/

# Copy libs directory (messaging-common, messaging-network)
COPY libs libs/

# Copy source code
COPY src src/

# Build application with shadowJar to create fat JAR
RUN ./gradlew shadowJar -x test --no-daemon

# Stage 2: Runtime
FROM eclipse-temurin:17-jre-jammy

# Create non-root user
RUN groupadd -r consumer && useradd -r -g consumer consumer

WORKDIR /app

# Copy built fat JAR from builder stage (shadow creates -all.jar)
COPY --from=builder /build/build/libs/*-all.jar app.jar

# Create data directory with proper permissions
RUN mkdir -p /app/consumer-data && chown -R consumer:consumer /app

# Switch to non-root user
USER consumer

# Environment variables with defaults
ENV CONSUMER_TYPE=default \
    CONSUMER_TOPICS=default-topic \
    CONSUMER_GROUP=default-group \
    CONSUMER_PORT=8080 \
    BROKER_HOST=localhost \
    BROKER_PORT=9092 \
    STORAGE_DATA_DIR=/app/consumer-data \
    JAVA_OPTS="-Xms64m -Xmx200m -XX:+UseG1GC"

# Expose port (configurable via CONSUMER_PORT)
EXPOSE ${CONSUMER_PORT}

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:${CONSUMER_PORT}/health || exit 1

# Run the application
CMD ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
