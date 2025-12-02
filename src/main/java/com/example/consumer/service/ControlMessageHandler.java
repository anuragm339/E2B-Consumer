package com.example.consumer.service;

import com.messaging.common.api.NetworkClient;
import com.messaging.common.model.BrokerMessage;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles RESET and READY control messages from broker
 */
@Singleton
public class ControlMessageHandler {
    private static final Logger log = LoggerFactory.getLogger(ControlMessageHandler.class);

    @Inject
    private ConsumerStorageService storageService;

    @Inject
    private SegmentMetadataService metadata;

    @Value("${consumer.type}")
    private String consumerType;

    /**
     * Handle RESET message from broker
     * Flow: Broker sends RESET → Consumer clears data → Consumer sends ACK
     */
    public void handleReset(BrokerMessage resetMessage, NetworkClient.Connection connection) throws Exception {
        log.info("[{}] ========================================", consumerType);
        log.info("[{}] Received RESET message", consumerType);
        log.info("[{}] ========================================", consumerType);

        // Get stats before clearing
        SegmentMetadataService.SegmentStats stats = metadata.getStats();
        log.info("[{}] Before RESET:", consumerType);
        log.info("[{}]   - Segments: {}", consumerType, stats.segmentCount());
        log.info("[{}]   - Records: {}", consumerType, stats.totalRecords());
        log.info("[{}]   - Offset range: [{}, {}]", consumerType, stats.minOffset(), stats.maxOffset());
        log.info("[{}]   - SQLite size: {} bytes ({} KB)",
                 consumerType, stats.dbSizeBytes(), stats.dbSizeBytes() / 1024);

        // Clear segments AND SQLite metadata
        storageService.clearAllData();

        log.info("[{}] Database cleared (segments + SQLite)", consumerType);

        // Send ACK
        sendAck(connection, resetMessage);

        log.info("[{}] ACK sent, waiting for data replay...", consumerType);
        log.info("[{}] ========================================", consumerType);
    }

    /**
     * Handle READY message from broker
     * Flow: Broker sends READY → Consumer validates data → Consumer sends ACK
     */
    public void handleReady(BrokerMessage readyMessage, NetworkClient.Connection connection) throws Exception {
        log.info("[{}] ========================================", consumerType);
        log.info("[{}] Received READY - Data refresh complete", consumerType);
        log.info("[{}] ========================================", consumerType);

        // Flush any pending segment metadata
        storageService.flushCurrentSegment();

        // Get stats after refresh
        SegmentMetadataService.SegmentStats stats = metadata.getStats();
        log.info("[{}] After refresh:", consumerType);
        log.info("[{}]   - Segments: {}", consumerType, stats.segmentCount());
        log.info("[{}]   - Records: {}", consumerType, stats.totalRecords());
        log.info("[{}]   - Offset range: [{}, {}]", consumerType, stats.minOffset(), stats.maxOffset());
        log.info("[{}]   - SQLite size: {} bytes ({} KB)",
                 consumerType, stats.dbSizeBytes(), stats.dbSizeBytes() / 1024);

        // Send ACK
        sendAck(connection, readyMessage);

        log.info("[{}] Data refresh complete! Normal flow resuming.", consumerType);
        log.info("[{}] ========================================", consumerType);
    }

    private void sendAck(NetworkClient.Connection connection, BrokerMessage originalMessage) throws Exception {
        BrokerMessage ack = new BrokerMessage(
            BrokerMessage.MessageType.ACK,
            originalMessage.getMessageId(),
            new byte[0]
        );

        connection.send(ack).get();
        log.info("[{}] Sent ACK for message {}", consumerType, originalMessage.getMessageId());
    }
}
