package com.example.consumer.service;

import com.messaging.common.api.StorageEngine;
import com.messaging.common.model.ConsumerRecord;
import com.messaging.common.model.EventType;
import com.messaging.common.model.MessageRecord;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Consumer storage service that uses segments for data and SQLite for metadata
 */
@Singleton
public class ConsumerStorageService {
    private static final Logger log = LoggerFactory.getLogger(ConsumerStorageService.class);

    private final StorageEngine storage;
    private final SegmentMetadataService metadata;
    private final String consumerType;
    private final String dataDir;

    // Track current segment for metadata updates
    private volatile int currentSegmentId = 0;
    private volatile long currentSegmentStartOffset = 0;
    private volatile int recordsInCurrentSegment = 0;

    @Value("${storage.segment-size:1000000}")
    private long segmentSize; // Records per segment

    @Inject
    public ConsumerStorageService(
            StorageEngine storage,
            SegmentMetadataService metadata,
            @Value("${consumer.type}") String consumerType,
            @Value("${storage.data-dir}") String dataDir) {
        this.storage = storage;
        this.metadata = metadata;
        this.consumerType = consumerType;
        this.dataDir = dataDir;
    }

    /**
     * Store message and update metadata when segment rolls
     */
    public synchronized void storeMessage(ConsumerRecord record) throws Exception {
        String consumerTopic = "consumer-" + consumerType + "-data";

        // Append to segment
        long offset = storage.append(consumerTopic, 0, toMessageRecord(record));

        recordsInCurrentSegment++;

        // Check if we rolled to a new segment
        int newSegmentId = (int) (offset / segmentSize);

        if (newSegmentId > currentSegmentId) {
            // Finalize previous segment in metadata
            finalizeSegment(currentSegmentId, offset - 1);

            // Start tracking new segment
            currentSegmentId = newSegmentId;
            currentSegmentStartOffset = offset;
            recordsInCurrentSegment = 1;
        }

        log.debug("[{}] Stored: offset={}, segment={}, key={}",
                 consumerType, offset, currentSegmentId, record.getMsgKey());
    }

    /**
     * Finalize segment metadata when it's full
     */
    private void finalizeSegment(int segmentId, long endOffset) throws Exception {
        String segmentFile = String.format("segment-%019d.dat", segmentId * segmentSize);

        metadata.registerSegment(
            segmentId,
            segmentFile,
            currentSegmentStartOffset,
            endOffset,
            recordsInCurrentSegment
        );

        log.info("[{}] Finalized segment {}: offsets [{}, {}], {} records",
                 consumerType, segmentId, currentSegmentStartOffset, endOffset, recordsInCurrentSegment);
    }

    /**
     * Find message by offset using SQLite metadata
     */
    public MessageRecord findByOffset(long offset) throws Exception {
        // Query SQLite for segment
        SegmentMetadataService.SegmentInfo segmentInfo = metadata.findSegmentByOffset(offset);

        if (segmentInfo == null) {
            log.warn("[{}] No segment found for offset: {}", consumerType, offset);
            return null;
        }

        log.debug("[{}] Found in segment {}: {}", consumerType, segmentInfo.segmentId(), segmentInfo.segmentFile());

        // Read from that specific segment
        String consumerTopic = "consumer-" + consumerType + "-data";
        List<MessageRecord> records = storage.read(consumerTopic, 0, offset, 1);

        return records.isEmpty() ? null : records.get(0);
    }

    /**
     * Get current record count
     */
    public long getRecordCount() throws Exception {
        return metadata.getCurrentOffset() + 1;
    }

    /**
     * Clear all data (segments + metadata)
     */
    public void clearAllData() throws Exception {
        String consumerTopic = "consumer-" + consumerType + "-data";

        // Clear SQLite metadata
        metadata.clearAllMetadata();

        // Delete segment files
        deleteSegmentFiles(consumerTopic);

        // Reset tracking
        currentSegmentId = 0;
        currentSegmentStartOffset = 0;
        recordsInCurrentSegment = 0;

        log.info("[{}] RESET: Cleared all segments and metadata", consumerType);
    }

    /**
     * Flush current segment metadata (called before shutdown or on READY)
     */
    public void flushCurrentSegment() throws Exception {
        if (recordsInCurrentSegment > 0) {
            long currentOffset = storage.getCurrentOffset("consumer-" + consumerType + "-data", 0);
            finalizeSegment(currentSegmentId, currentOffset);
        }
    }

    private MessageRecord toMessageRecord(ConsumerRecord record) {
        return new MessageRecord(
            record.getMsgKey(),
            record.getEventType(),
            record.getData(),
            record.getCreatedAt()
        );
    }

    private void deleteSegmentFiles(String topic) {
        try {
            // Path: {dataDir}/{topic}/partition-0/
            Path topicPath = Paths.get(dataDir, topic, "partition-0");

            if (Files.exists(topicPath)) {
                File dir = topicPath.toFile();
                File[] files = dir.listFiles((d, name) -> name.startsWith("segment-"));

                if (files != null) {
                    for (File file : files) {
                        boolean deleted = file.delete();
                        if (deleted) {
                            log.debug("[{}] Deleted segment file: {}", consumerType, file.getName());
                        }
                    }
                }

                log.info("[{}] Deleted {} segment files", consumerType, files != null ? files.length : 0);
            }
        } catch (Exception e) {
            log.error("[{}] Error deleting segment files", consumerType, e);
        }
    }
}
