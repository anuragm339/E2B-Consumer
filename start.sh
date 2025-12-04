#!/bin/sh
echo "=============================================="
echo "Starting Consumer Application"
echo "=============================================="
echo "Consumer Type: ${CONSUMER_TYPE}"
echo "Topics:        ${CONSUMER_TOPICS:-${CONSUMER_TOPIC}}"
echo "Group:         ${CONSUMER_GROUP}"
echo "Port:          ${CONSUMER_PORT}"
echo "Retry Policy:  ${CONSUMER_RETRY_POLICY}"
echo "Broker:        ${BROKER_HOST}:${BROKER_PORT}"
echo "Data Dir:      ${STORAGE_DATA_DIR}"
echo "Segment Size:  ${STORAGE_SEGMENT_SIZE} records"
echo "=============================================="

# Start application with Micronaut classpath
exec java -cp /app/resources:/app/classes:/app/libs/*:/app/application.jar com.example.consumer.ConsumerApplication
