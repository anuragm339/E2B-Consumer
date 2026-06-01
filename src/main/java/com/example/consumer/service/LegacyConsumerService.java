package com.example.consumer.service;

import com.example.consumer.config.LegacyConfig;
import com.example.consumer.legacy.events.*;
import com.example.consumer.store.ReceivedMessageStore;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.runtime.server.event.ServerStartupEvent;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Legacy consumer service - only activated when consumer.legacy.enabled=true
 * Bypasses the @Consumer framework and uses direct LegacyBrokerConnection
 */
@Singleton
@Requires(property = "consumer.legacy.enabled", value = "true")
public class LegacyConsumerService implements ApplicationEventListener<ServerStartupEvent> {
    private static final Logger log = LoggerFactory.getLogger(LegacyConsumerService.class);

    private final LegacyConfig legacyConfig;
    private final String brokerHost;
    private final int brokerPort;
    private final ReceivedMessageStore receivedMessageStore;
    private final Map<String, Long> lastSeenOffsetByTopic = new ConcurrentHashMap<>();

    private LegacyBrokerConnection connection;
    private volatile boolean running = false;
    private Thread eventLoopThread;

    @Inject
    public LegacyConsumerService(
            LegacyConfig legacyConfig,
            @Value("${messaging.broker.host}") String brokerHost,
            @Value("${messaging.broker.port}") int brokerPort,
            ReceivedMessageStore receivedMessageStore) {

        this.legacyConfig = legacyConfig;
        this.brokerHost = brokerHost;
        this.brokerPort = brokerPort;
        this.receivedMessageStore = receivedMessageStore;

        log.info("🔧 LegacyConsumerService created - will start on ServerStartupEvent");
    }

    @Override
    public void onApplicationEvent(ServerStartupEvent event) {
        log.info("═══════════════════════════════════════════════════");
        log.info("🚀 LEGACY MODE ENABLED - Using Event Protocol");
        log.info("   Service: {}", legacyConfig.getServiceName());
        log.info("   Broker: {}:{}", brokerHost, brokerPort);
        log.info("═══════════════════════════════════════════════════");

        try {
            connect();
        } catch (Exception e) {
            log.error("❌ Failed to start legacy consumer", e);
            throw new RuntimeException("Failed to start legacy consumer", e);
        }
    }

    private void connect() throws Exception {
        connection = new LegacyBrokerConnection(legacyConfig.getServiceName());
        connection.connect(brokerHost, brokerPort);

        running = true;
        eventLoopThread = new Thread(this::runEventLoop, "LegacyConsumer-EventLoop");
        eventLoopThread.setDaemon(false);  // Keep application alive
        eventLoopThread.start();

        log.info("✅ Legacy consumer started successfully");
    }

    private void runEventLoop() {
        log.info("🔄 Event loop started");

        while (running) {
            try {
                Event event = connection.nextEvent();
                log.debug("📨 Received event: {}", event.getType());

                handleEvent(event);

            } catch (Exception e) {
                if (running) {
                    log.error("❌ Error in event loop", e);
                    // Optionally: implement reconnection logic here
                }
                running = false;
            }
        }

        log.info("🛑 Event loop stopped");
    }

    private void handleEvent(Event event) throws Exception {
        boolean shouldAck = false;

        switch (event.getType()) {
            case BATCH:
                shouldAck = handleBatch((BatchEvent) event);
                break;

            case MESSAGE:
                shouldAck = handleMessage((DataMessageEvent) event);
                break;

            case RESET:
                shouldAck = handleReset((ResetEvent) event);
                break;

            case READY:
                shouldAck = handleReady((ReadyEvent) event);
                break;

            case EOF:
                log.info("📪 EOF received - shutting down");
                running = false;
                break;

            default:
                log.warn("⚠️ Unexpected event type: {}", event.getType());
        }

        if (shouldAck) {
            connection.sendAck();
        }
    }

    private boolean handleBatch(BatchEvent batchEvent) throws Exception {
        int size = batchEvent.count();
        log.info("📦 BATCH received: size={}", size);

        java.util.Map<String, List<String>> keysByTopic = new java.util.LinkedHashMap<>();
        java.util.Map<String, List<String>> eventTypesByTopic = new java.util.LinkedHashMap<>();
        java.util.Map<String, Long> minOffsetByTopic = new java.util.LinkedHashMap<>();
        java.util.Map<String, Long> maxOffsetByTopic = new java.util.LinkedHashMap<>();
        java.util.Map<String, Integer> countByTopic = new java.util.LinkedHashMap<>();

        for (Event e : batchEvent.getEvents()) {
            if (e.getType() == EventType.MESSAGE) {
                DataMessageEvent msgEvent = (DataMessageEvent) e;
                DataMessage msg = msgEvent.getMessage();
                long offset = msgEvent.getOffset();
                keysByTopic.computeIfAbsent(msg.getType(), t -> new ArrayList<>()).add(msg.getKey());
                eventTypesByTopic.computeIfAbsent(msg.getType(), t -> new ArrayList<>()).add("MESSAGE");
                minOffsetByTopic.merge(msg.getType(), offset, Math::min);
                maxOffsetByTopic.merge(msg.getType(), offset, Math::max);
                countByTopic.merge(msg.getType(), 1, Integer::sum);
                log.debug("  Message: type={}, key={}, offset={}", msg.getType(), msg.getKey(), offset);
            } else if (e.getType() == EventType.DELETE) {
                DeleteMessageEvent delEvent = (DeleteMessageEvent) e;
                Message msg = delEvent.getMessage();
                long offset = delEvent.getOffset();
                keysByTopic.computeIfAbsent(msg.getType(), t -> new ArrayList<>()).add(msg.getKey());
                eventTypesByTopic.computeIfAbsent(msg.getType(), t -> new ArrayList<>()).add("DELETE");
                minOffsetByTopic.merge(msg.getType(), offset, Math::min);
                maxOffsetByTopic.merge(msg.getType(), offset, Math::max);
                countByTopic.merge(msg.getType(), 1, Integer::sum);
                log.debug("  DELETE: type={}, key={}, offset={}", msg.getType(), msg.getKey(), offset);
            }
        }

        for (String topic : countByTopic.keySet()) {
            long minOffset = minOffsetByTopic.getOrDefault(topic, -1L);
            long maxOffset = maxOffsetByTopic.getOrDefault(topic, -1L);
            int topicCount = countByTopic.getOrDefault(topic, 0);
            Long previousMax = lastSeenOffsetByTopic.put(topic, maxOffset);

            log.info("event=legacy_batch.topic_summary service={} topic={} count={} minOffset={} maxOffset={} previousMaxOffset={}",
                    legacyConfig.getServiceName(), topic, topicCount, minOffset, maxOffset,
                    previousMax != null ? previousMax : -1L);

            if (previousMax != null && maxOffset <= previousMax) {
                log.warn("event=legacy_batch.stale_offset_detected service={} topic={} previousMaxOffset={} batchMaxOffset={} batchMinOffset={}",
                        legacyConfig.getServiceName(), topic, previousMax, maxOffset, minOffset);
            }
        }

        for (java.util.Map.Entry<String, List<String>> entry : keysByTopic.entrySet()) {
            String topic = entry.getKey();
            List<String> keys = entry.getValue();
            List<String> types = eventTypesByTopic.get(topic);
            if (!keys.isEmpty()) {
                receivedMessageStore.storeBatch(topic, legacyConfig.getServiceName(), keys, types, null);
            }
        }

        return true;  // Send ACK
    }

    private boolean handleMessage(DataMessageEvent event) throws Exception {
        DataMessage msg = event.getMessage();
        log.debug("📨 MESSAGE received: type={}, key={}", msg.getType(), msg.getKey());
        return true;  // Send ACK
    }

    private boolean handleReset(ResetEvent event) throws Exception {
        log.info("🔄 RESET received - clearing local data");
        // Call GenericConsumerHandler.onReset() if needed
        // messageHandler.onReset("all-topics");
        return true;  // Send ACK
    }

    private boolean handleReady(ReadyEvent event) throws Exception {
        log.info("✅ READY received - refresh complete");
        return true;  // Send ACK
    }

    public void shutdown() {
        log.info("🛑 Shutting down legacy consumer...");
        running = false;

        try {
            if (connection != null) {
                connection.close();
            }

            if (eventLoopThread != null) {
                eventLoopThread.join(5000);
            }
        } catch (Exception e) {
            log.error("❌ Error during shutdown", e);
        }

        log.info("✅ Legacy consumer shutdown complete");
    }
}
