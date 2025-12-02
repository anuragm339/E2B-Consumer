#!/bin/bash
# Build the consumer-app Docker image

echo "=============================================="
echo "Building Consumer Application Docker Image"
echo "=============================================="

# Step 1: Copy JAR files from provider project
echo "Step 1: Copying JAR files from provider project..."
cp ../provider/common/build/libs/common.jar libs/
cp ../provider/storage/build/libs/storage.jar libs/
cp ../provider/network/build/libs/network.jar libs/

echo "  ✓ common.jar"
echo "  ✓ storage.jar"
echo "  ✓ network.jar"

# Step 2: Build consumer-app JAR
echo ""
echo "Step 2: Building consumer-app JAR..."
./gradlew build -x test

if [ $? -ne 0 ]; then
    echo "❌ Build failed!"
    exit 1
fi

echo "  ✓ consumer-app.jar built"

# Step 3: Build Docker image
echo ""
echo "Step 3: Building Docker image..."
docker build -t consumer-app:latest .

if [ $? -ne 0 ]; then
    echo "❌ Docker build failed!"
    exit 1
fi

echo "  ✓ Docker image built: consumer-app:latest"

echo ""
echo "=============================================="
echo "Build complete!"
echo "=============================================="
echo ""
echo "Next steps:"
echo "  - Start all consumers: docker-compose up -d"
echo "  - Start custom consumer: ./scripts/start-custom-consumer.sh <type> <topic> <port>"
echo ""
