#!/bin/bash
# Usage: ./start-custom-consumer.sh <type> <topic> <port> [retry-policy]
#
# Examples:
#   ./start-custom-consumer.sh price price-topic 8081
#   ./start-custom-consumer.sh shipping shipping-topic 8090 SKIP_ON_ERROR

CONSUMER_TYPE=$1
CONSUMER_TOPIC=$2
CONSUMER_PORT=$3
CONSUMER_RETRY_POLICY=${4:-EXPONENTIAL_THEN_FIXED}

if [ -z "$CONSUMER_TYPE" ] || [ -z "$CONSUMER_TOPIC" ] || [ -z "$CONSUMER_PORT" ]; then
    echo "Usage: $0 <type> <topic> <port> [retry-policy]"
    echo ""
    echo "Arguments:"
    echo "  type         - Consumer identifier (e.g., price, product, inventory)"
    echo "  topic        - Topic to subscribe to (e.g., price-topic)"
    echo "  port         - HTTP port for this consumer (e.g., 8081)"
    echo "  retry-policy - Optional retry policy (default: EXPONENTIAL_THEN_FIXED)"
    echo "                 Options: EXPONENTIAL_THEN_FIXED, SKIP_ON_ERROR, PAUSE_ON_ERROR"
    echo ""
    echo "Examples:"
    echo "  $0 price price-topic 8081"
    echo "  $0 shipping shipping-topic 8090 SKIP_ON_ERROR"
    exit 1
fi

echo "=============================================="
echo "Starting Custom Consumer"
echo "=============================================="
echo "Type:         $CONSUMER_TYPE"
echo "Topic:        $CONSUMER_TOPIC"
echo "Port:         $CONSUMER_PORT"
echo "Retry Policy: $CONSUMER_RETRY_POLICY"
echo "=============================================="

docker run -d \
  --name ${CONSUMER_TYPE}-consumer \
  --network messaging-network \
  -p ${CONSUMER_PORT}:${CONSUMER_PORT} \
  -v ${CONSUMER_TYPE}-consumer-data:/app/consumer-data \
  -e CONSUMER_TYPE=${CONSUMER_TYPE} \
  -e CONSUMER_TOPIC=${CONSUMER_TOPIC} \
  -e CONSUMER_GROUP=${CONSUMER_TYPE}-group \
  -e CONSUMER_PORT=${CONSUMER_PORT} \
  -e CONSUMER_RETRY_POLICY=${CONSUMER_RETRY_POLICY} \
  -e BROKER_HOST=broker \
  -e BROKER_PORT=9092 \
  -e STORAGE_DATA_DIR=/app/consumer-data \
  -e STORAGE_SEGMENT_SIZE=1000000 \
  consumer-app:latest

echo ""
echo "Consumer started!"
echo "  Container: ${CONSUMER_TYPE}-consumer"
echo "  Port:      ${CONSUMER_PORT}"
echo "  Volume:    ${CONSUMER_TYPE}-consumer-data"
echo ""
echo "Check logs:  docker logs -f ${CONSUMER_TYPE}-consumer"
echo "Query API:   curl http://localhost:${CONSUMER_PORT}/api/query/stats"
echo "=============================================="
