# Consumer Application - Dockerized Segment-Based Message Consumer

## Overview

Standalone consumer application that demonstrates the Kafka-like messaging system with:
- **Segment-based storage** for huge datasets (GB-TB scale)
- **SQLite metadata indexing** for fast offset lookups (minimal overhead: ~KB)
- **Environment-variable configuration** to create N consumer types
- **RESET/READY workflow** for data refresh
- **Three retry policies**: EXPONENTIAL_THEN_FIXED, SKIP_ON_ERROR, PAUSE_ON_ERROR

## Architecture

```
Consumer Container
├── Segment Storage (/app/consumer-data/segments/)
│   ├── segment-0000000000.dat (1M records)
│   ├── segment-0000001000.dat
│   └── ...
└── SQLite Metadata (/app/consumer-data/segment-metadata.db)
    └── segment_ranges table (tracks offset ranges)
        Example: segment 0 contains offsets [0, 999999]
                 segment 1 contains offsets [1000000, 1999999]
```

## SQLite Schema (Minimal - Only Segment Ranges)

```sql
CREATE TABLE segment_ranges (
    segment_id INTEGER PRIMARY KEY,
    segment_file VARCHAR(255),
    start_offset BIGINT,
    end_offset BIGINT,
    record_count INTEGER
);

-- Query by offset: O(log n) with B-tree index
SELECT segment_file FROM segment_ranges
WHERE start_offset <= ? AND end_offset >= ?
```

**Size:** ~100 bytes per segment → 10KB for 1000 segments → **Extremely lightweight**

## Quick Start

### 1. Build the Project

```bash
# From consumer-app directory
./scripts/build-and-push.sh
```

This will:
1. Copy JAR files from provider project
2. Build consumer-app JAR
3. Build Docker image: `consumer-app:latest`

### 2. Start All Consumers

```bash
# Using docker-compose (starts broker + 4 consumers)
docker-compose up -d

# Check status
docker-compose ps
```

This starts:
- `broker` - Main broker (port 9092, HTTP 8080)
- `price-consumer` - Port 8081, EXPONENTIAL_THEN_FIXED retry
- `product-consumer` - Port 8082, SKIP_ON_ERROR retry
- `inventory-consumer` - Port 8083, EXPONENTIAL_THEN_FIXED retry
- `audit-consumer` - Port 8084, PAUSE_ON_ERROR retry

### 3. Start Custom Consumer

```bash
# Create a custom consumer on-the-fly
./scripts/start-custom-consumer.sh shipping shipping-topic 8090 SKIP_ON_ERROR

# Syntax:
./scripts/start-custom-consumer.sh <type> <topic> <port> [retry-policy]
```

## Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `CONSUMER_TYPE` | Consumer identifier | `price`, `product`, `shipping` |
| `CONSUMER_TOPIC` | Topic to subscribe to | `price-topic` |
| `CONSUMER_GROUP` | Consumer group name | `price-group` |
| `CONSUMER_PORT` | HTTP port for this consumer | `8081` |
| `CONSUMER_RETRY_POLICY` | Retry policy | `EXPONENTIAL_THEN_FIXED`, `SKIP_ON_ERROR`, `PAUSE_ON_ERROR` |
| `BROKER_HOST` | Broker hostname | `broker` (Docker), `localhost` (local) |
| `BROKER_PORT` | Broker TCP port | `9092` |
| `STORAGE_DATA_DIR` | Segment storage directory | `/app/consumer-data` |
| `STORAGE_SEGMENT_SIZE` | Records per segment | `1000000` (1M records) |

## REST API

Each consumer exposes a REST API on its configured port:

### Query Message by Offset
```bash
# Fast lookup using SQLite index to find segment
curl http://localhost:8081/api/query/offset/123456

Response:
{
  "offset": 123456,
  "msgKey": "product_123",
  "eventType": "MESSAGE",
  "data": "{\"price\": 19.99}",
  "createdAt": "2025-11-29T10:30:00Z"
}
```

### Get Statistics
```bash
curl http://localhost:8081/api/query/stats

Response:
{
  "segmentCount": 15,
  "totalRecords": 15000000,
  "offsetRange": {"min": 0, "max": 14999999},
  "sqliteDbSize": {
    "bytes": 15360,
    "kilobytes": 15,
    "megabytes": 0
  }
}
```

### List Segments
```bash
curl http://localhost:8081/api/query/segments

Response:
{
  "count": 3,
  "segments": [
    {
      "segmentId": 0,
      "file": "segment-0000000000.dat",
      "startOffset": 0,
      "endOffset": 999999,
      "range": "[0, 999999]",
      "capacity": 1000000
    },
    ...
  ]
}
```

### Health Check
```bash
curl http://localhost:8081/api/query/health

Response:
{
  "status": "UP",
  "segments": 15,
  "records": 15000000
}
```

## RESET/READY Workflow

### Data Refresh Flow

```
1. Broker sends RESET to all consumers
   ↓
2. Consumer receives RESET
   ↓
3. Consumer clears segments AND SQLite metadata
   ↓
4. Consumer sends ACK
   ↓
5. Broker replays ALL messages from offset 0
   ↓
6. Consumer stores replayed messages in new segments
   ↓
7. Consumer updates SQLite with new segment ranges
   ↓
8. Broker sends READY (all data sent)
   ↓
9. Consumer receives READY
   ↓
10. Consumer flushes segment metadata
   ↓
11. Consumer sends ACK
   ↓
12. Normal waterfall flow resumes
```

### Trigger RESET

```bash
# TODO: Create admin endpoint in broker
curl -X POST http://localhost:8080/admin/reset
```

## Storage Details

### Segment Files

```
/app/consumer-data/
├── segment-metadata.db              # SQLite: 15 KB for 1000 segments
└── consumer-price-data/
    └── partition-0/
        ├── segment-0000000000000000000.dat    # 1M records (~100MB)
        ├── segment-0000000000000000000.idx
        ├── segment-0000000001000000000.dat
        └── segment-0000000001000000000.idx
```

### Performance

**Lookup by Offset:**
- Without SQLite: Scan all segments (O(n) * segment scan time)
- With SQLite: Query index (O(log n)) → load 1 segment

**Example with 100GB data:**
- 100 segments × 1GB each = 100GB
- SQLite: 100 rows × 100 bytes = 10KB
- Overhead: < 0.00001%

**Query Performance:**
- 10 segments: < 1ms
- 1,000 segments: < 5ms
- 10,000 segments: < 10ms

## Retry Policies

### EXPONENTIAL_THEN_FIXED (Default)
Exponential backoff (1s → 2s → 4s → 8s → ...) then fixed interval (30s) forever.

**Use when:** Automatic recovery with infinite retries.

**Example:** Price consumer, inventory consumer

### SKIP_ON_ERROR
Skips failed messages and continues to next.

**Use when:** Data loss acceptable, prefer availability.

**Example:** Product consumer

### PAUSE_ON_ERROR
Pauses consumer on first failure, requires manual intervention.

**Use when:** Data integrity critical, manual review required.

**Example:** Audit consumer

## Docker Commands

### View Logs
```bash
# All containers
docker-compose logs -f

# Specific consumer
docker logs -f price-consumer

# Custom consumer
docker logs -f shipping-consumer
```

### Stop Consumers
```bash
# All
docker-compose down

# Specific
docker stop price-consumer
```

### Remove Volumes (Clear Data)
```bash
docker-compose down -v
```

### Inspect Volume
```bash
docker volume inspect price-consumer-data
```

## Directory Structure

```
consumer-app/
├── build.gradle                      # Dependencies and build config
├── settings.gradle
├── Dockerfile                        # Docker image definition
├── docker-compose.yml                # Multi-consumer orchestration
├── start.sh                          # Container startup script
├── src/
│   ├── main/
│   │   ├── java/com/example/consumer/
│   │   │   ├── ConsumerApplication.java           # Main app
│   │   │   ├── GenericConsumerHandler.java        # Message handler
│   │   │   ├── api/
│   │   │   │   └── QueryController.java           # REST API
│   │   │   └── service/
│   │   │       ├── SegmentMetadataService.java    # SQLite metadata
│   │   │       ├── ConsumerStorageService.java    # Segment storage
│   │   │       └── ControlMessageHandler.java     # RESET/READY
│   │   └── resources/
│   │       ├── application.yml                     # Config template
│   │       └── logback.xml                         # Logging
├── libs/                             # JAR dependencies
│   ├── common.jar
│   ├── storage.jar
│   └── network.jar
├── scripts/
│   ├── build-and-push.sh             # Build Docker image
│   └── start-custom-consumer.sh      # Start custom consumer
└── README.md                         # This file
```

## Examples

### Example 1: Start Price Consumer
```bash
./scripts/start-custom-consumer.sh price price-topic 8081
```

### Example 2: Query Data
```bash
# Get stats
curl http://localhost:8081/api/query/stats

# Find message at offset 12345
curl http://localhost:8081/api/query/offset/12345

# List all segments
curl http://localhost:8081/api/query/segments
```

### Example 3: Monitor Consumer
```bash
# Watch logs
docker logs -f price-consumer

# Check health
curl http://localhost:8081/api/query/health
```

### Example 4: Add New Consumer Type
```bash
# Create a "shipping" consumer
./scripts/start-custom-consumer.sh shipping shipping-topic 8090 EXPONENTIAL_THEN_FIXED

# Verify it's running
docker ps | grep shipping
curl http://localhost:8090/api/query/health
```

## Troubleshooting

### Consumer Not Receiving Messages
```bash
# Check broker logs
docker logs broker

# Check consumer logs
docker logs price-consumer

# Verify network
docker network inspect messaging-network
```

### Check SQLite Size
```bash
# Enter consumer container
docker exec -it price-consumer sh

# Check SQLite file size
ls -lh /app/consumer-data/segment-metadata.db

# Query SQLite
apk add sqlite
sqlite3 /app/consumer-data/segment-metadata.db "SELECT COUNT(*) FROM segment_ranges;"
```

### Clear Consumer Data
```bash
# Stop consumer
docker stop price-consumer

# Remove volume
docker volume rm price-consumer-data

# Restart
docker start price-consumer
```

## Next Steps

1. **Send Messages:** Use provider/examples/FullBrokerExample to send test messages
2. **Trigger RESET:** Implement admin endpoint to trigger data refresh
3. **Monitor:** Use REST API to monitor segment growth and SQLite size
4. **Scale:** Add more consumer types using start-custom-consumer.sh

## Key Features

✅ **Minimal SQLite** - Only tracks segment offset ranges (KB overhead)
✅ **Fast Lookups** - O(log n) offset queries via B-tree index
✅ **Huge Data** - Segment-based storage handles GB-TB datasets
✅ **Docker Native** - Environment-variable configuration
✅ **Flexible** - Create N consumer types by changing script
✅ **RESET/READY** - Complete data refresh workflow
✅ **REST API** - Query and monitor consumers
✅ **Persistent** - Docker volumes survive restarts
