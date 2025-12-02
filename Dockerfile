FROM eclipse-temurin:17-jre

WORKDIR /app

# Copy Micronaut layered build
COPY build/docker/main/layers/libs /app/libs
COPY build/docker/main/layers/classes /app/classes
COPY build/docker/main/layers/resources /app/resources
COPY build/docker/main/layers/application.jar /app/application.jar

# Copy our messaging JARs
COPY libs/*.jar /app/libs/

# Copy startup script
COPY start.sh /app/start.sh
RUN chmod +x /app/start.sh

# Create data directory for segments
RUN mkdir -p /app/consumer-data

# Expose port (will be overridden by ENV)
EXPOSE 8080

# Volume for persistent segment storage
VOLUME /app/consumer-data

ENTRYPOINT ["/app/start.sh"]
