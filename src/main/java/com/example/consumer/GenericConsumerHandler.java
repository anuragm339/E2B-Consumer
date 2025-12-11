package com.example.consumer;


import com.messaging.common.annotation.Consumer;
import com.messaging.common.annotation.RetryPolicy;
import com.messaging.common.api.MessageHandler;
import com.messaging.common.model.ConsumerRecord;
import com.messaging.common.model.EventType;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Generic consumer handler that adapts based on environment variables.
 * Single class handles all consumer types (price, product, inventory, etc.)
 */
@Singleton
@Consumer(
    topic = "${CONSUMER_TOPIC:price-topic}",
    group = "${CONSUMER_GROUP:price-group}"
    // RetryPolicy will be set via RetryPolicyProvider
)
public class GenericConsumerHandler implements MessageHandler {
    private static final Logger log = LoggerFactory.getLogger(GenericConsumerHandler.class);

    @Value("${consumer.type}")
    private String consumerType;

    @Override
    public void handleBatch(List<ConsumerRecord> records) throws Exception {
        log.info("[{}] Received batch of {} records", consumerType, records.size());

        for (ConsumerRecord record : records) {
            log.debug("[{}] Processing: type={}, key={}, createdAt={}",
                     consumerType, record.getEventType(), record.getMsgKey(), record.getCreatedAt());

            switch (record.getEventType()) {
                case MESSAGE -> handleMessage(record);
                case DELETE -> handleDelete(record);
            }
        }

        log.info("[{}] Successfully processed batch of {} records", consumerType, records.size());
    }

    private void handleMessage(ConsumerRecord record) throws Exception {
        // Store MESSAGE in consumer's segments


        // Process business logic specific to consumer type
        processBusinessLogic(record);

        //log.info("[{}] Processed MESSAGE: key={}", consumerType, record.getMsgKey());
    }

    private void handleDelete(ConsumerRecord record) throws Exception {
        // Store DELETE (tombstone) in consumer's segments


        // Handle deletion logic
        handleDeletion(record.getMsgKey());

        //log.info("[{}] Processed DELETE: key={}", consumerType, record.getMsgKey());
    }

    private void processBusinessLogic(ConsumerRecord record) {
        // Business logic specific to consumer type
        // E.g., price consumer might update price index
        log.debug("[{}] Business logic for: {}", consumerType,record.getEventType(), record.getMsgKey());

    }

    private void processPriceUpdate(ConsumerRecord record) {
        // Price-specific logic (e.g., update price cache, trigger alerts)
        log.debug("[PRICE] Processing price update for: {}", record.getMsgKey());
    }

    private void processProductUpdate(ConsumerRecord record) {
        // Product-specific logic (e.g., update search index)
        log.debug("[PRODUCT] Processing product update for: {}", record.getMsgKey());
    }

    private void processInventoryUpdate(ConsumerRecord record) {
        // Inventory-specific logic (e.g., check stock levels, trigger reorder)
        log.debug("[INVENTORY] Processing inventory update for: {}", record.getMsgKey());
    }

    private void processAuditLog(ConsumerRecord record) {
        // Audit-specific logic (e.g., append to audit trail, compliance checks)
        log.debug("[AUDIT] Processing audit log for: {}", record.getMsgKey());
    }

    private void handleDeletion(String msgKey) {
        log.debug("[{}] Handling deletion for: {}", consumerType, msgKey);
    }
}
