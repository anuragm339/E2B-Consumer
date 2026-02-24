#!/bin/bash

# Script to run multiple consumer instances locally

# --- Configuration ---
JAR_PATH="build/libs/consumer-app-1.0.0-SNAPSHOT-all.jar"
BROKER_HOST=${BROKER_HOST:-"localhost"}
BROKER_PORT=${BROKER_PORT:-"9092"}
PID_FILE="consumer_pids.txt"

# --- Consumer Definitions ---
# Add or remove consumer configurations here
# Format: "CONSUMER_TYPE;CONSUMER_TOPICS;CONSUMER_GROUP;CONSUMER_PORT"
CONSUMERS=(
  "price-quote;prices-v1,reference-data-v5,non-promotable-products,prices-v4,minimum-price,deposit;price-quote-group;8090"
  "product-svc-lite;product-base-document;product-svc-lite-group;8091"
  "search-enterprise;search-product;search-enterprise-group;8092"
  "tesco-location-service;location,location-clusters;tesco-location-service-group;8093"
  "customer-order-on-till;selling-restrictions;customer-order-on-till-group;8094"
  "colleague-facts;colleague-facts-jobs,colleague-facts-legacy;colleague-facts-group;8095"
  "loss-prevention-api;loss-prevention-configuration,loss-prevention-store-configuration,loss-prevention-product,loss-prevention-rule-config;loss-prevention-api-group;8096"
)

# --- Functions ---

start_consumers() {
  if [ ! -f "$JAR_PATH" ]; then
    echo "Error: JAR file not found at $JAR_PATH"
    echo "Please build the project first using './gradlew shadowJar'"
    exit 1
  fi

  echo "Starting consumers..."
  rm -f "$PID_FILE"

  for consumer_config in "${CONSUMERS[@]}"; do
    IFS=';' read -r type topics group port <<< "$consumer_config"

    echo "----------------------------------------------"
    echo "Starting consumer: $type"
    echo "  Port:   $port"
    echo "  Topics: $topics"
    echo "  Group:  $group"
    echo "----------------------------------------------"

    # Set environment variables for the process
    export CONSUMER_TYPE="$type"
    export CONSUMER_TOPICS="$topics"
    export CONSUMER_GROUP="$group"
    export CONSUMER_PORT="$port"
    export BROKER_HOST="$BROKER_HOST"
    export BROKER_PORT="$BROKER_PORT"
    export STORAGE_DATA_DIR="./consumer-data/$type-data"

    # Create data directory
    mkdir -p "$STORAGE_DATA_DIR"

    # Start the java application in the background
    java -jar "$JAR_PATH" &

    # Save the PID of the background process
    echo $! >> "$PID_FILE"
  done

  echo "All consumers started. PIDs saved in $PID_FILE"
}

stop_consumers() {
  if [ -f "$PID_FILE" ]; then
    echo "Stopping consumers..."
    while read -r pid; do
      if ps -p "$pid" > /dev/null; then
        echo "Stopping process with PID: $pid"
        kill "$pid"
      else
        echo "Process with PID $pid not found."
      fi
    done < "$PID_FILE"
    rm "$PID_FILE"
    echo "All consumers stopped."
  else
    echo "No PID file found. Are the consumers running?"
  fi
}

# --- Main Logic ---

case "$1" in
  start)
    start_consumers
    ;;
  stop)
    stop_consumers
    ;;
  *)
    echo "Usage: $0 {start|stop}"
    exit 1
    ;;
esac
