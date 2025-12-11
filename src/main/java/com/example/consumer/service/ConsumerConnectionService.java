package com.example.consumer.service;

import com.example.consumer.GenericConsumerHandler;
import com.messaging.common.model.BrokerMessage;
import com.messaging.common.model.ConsumerRecord;
import com.messaging.network.tcp.NettyTcpClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import io.micronaut.context.annotation.Value;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.runtime.server.event.ServerStartupEvent;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Service that manages the TCP connection to the broker
 */
@Singleton
public class ConsumerConnectionService implements ApplicationEventListener<ServerStartupEvent> {
    private static final Logger log = LoggerFactory.getLogger(ConsumerConnectionService.class);

    @Value("${messaging.broker.host}")
    private String brokerHost;

    @Value("${messaging.broker.port}")
    private int brokerPort;

    @Value("${consumer.topics}")
    private String consumerTopics;  // Comma-separated list of topics

    @Value("${consumer.group}")
    private String consumerGroup;

    @Value("${consumer.type}")
    private String consumerType;

    @Inject
    private GenericConsumerHandler messageHandler;

    @Inject
    private ControlMessageHandler controlHandler;

    private final ObjectMapper objectMapper;
    private NettyTcpClient client;
    private NettyTcpClient.Connection connection;

    public ConsumerConnectionService() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.findAndRegisterModules(); // Register JSR310 module for Java 8 date/time
    }

    @Override
    public void onApplicationEvent(ServerStartupEvent event) {
        log.debug("[{}] Connecting to broker at {}:{}...", consumerType, brokerHost, brokerPort);

        try {
            // Create client and connect
            client = new NettyTcpClient();

            CompletableFuture<NettyTcpClient.Connection> connectFuture =
                client.connect(brokerHost, brokerPort);

            connection = connectFuture.join();

            // Set message handler
            connection.onMessage(this::handleMessage);

            log.info("[{}] Connected to broker successfully", consumerType);

            // Subscribe to topic
            subscribe();

        } catch (Exception e) {
            log.error("[{}] Failed to connect to broker", consumerType, e);
            throw new RuntimeException("Failed to connect to broker", e);
        }
    }

    private void subscribe() {
        try {
            // Parse comma-separated topics
            String[] topics = consumerTopics.split(",");

            log.info("[{}] Subscribing to {} topic(s): {}, group: {}",
                    consumerType, topics.length, consumerTopics, consumerGroup);

            // Subscribe to each topic
            for (String topic : topics) {
                topic = topic.trim();  // Remove any whitespace

                String payload = String.format("{\"topic\":\"%s\",\"group\":\"%s\"}",
                                              topic, consumerGroup);

                BrokerMessage subscribeMsg = new BrokerMessage(
                    BrokerMessage.MessageType.SUBSCRIBE,
                    System.currentTimeMillis(),
                    payload.getBytes(StandardCharsets.UTF_8)
                );

                final String currentTopic = topic;
                connection.send(subscribeMsg).whenComplete((v, ex) -> {
                    if (ex != null) {
                        log.error("[{}] Failed to send SUBSCRIBE for topic: {}", consumerType, currentTopic, ex);
                    } else {
                        log.info("[{}] SUBSCRIBE sent successfully for topic: {}", consumerType, currentTopic);
                    }
                });
            }

        } catch (Exception e) {
            log.error("[{}] Error subscribing to topics", consumerType, e);
        }
    }

    private void handleMessage(BrokerMessage message) {
        try {
            log.debug("[{}] Received message: type={}, id={}",
                     consumerType, message.getType(), message.getMessageId());

            switch (message.getType()) {
                case DATA:
                    handleDataMessage(message);
                    break;

                case ACK:
                    log.debug("[{}] Received ACK for message {}", consumerType, message.getMessageId());
                    break;

                case RESET:
                    controlHandler.handleReset(message, connection);
                    break;

                case READY:
                    controlHandler.handleReady(message, connection);
                    break;

                default:
                    log.warn("[{}] Unknown message type: {}", consumerType, message.getType());
            }

        } catch (Exception e) {
            log.error("[{}] Error handling message", consumerType, e);
        }
    }

    /**
     * Handle DATA message containing batch of consumer records
     */
    private void handleDataMessage(BrokerMessage message) {
        try {
            // Parse JSON payload as list of ConsumerRecords
            String json = new String(message.getPayload(), StandardCharsets.UTF_8);

            List<ConsumerRecord> records = objectMapper.readValue(
                json,
                new TypeReference<List<ConsumerRecord>>() {}
            );

            log.debug("[{}] Received batch of {} records", consumerType, records.size());

            // Deliver batch to message handler
            messageHandler.handleBatch(records);

            log.debug("[{}] Successfully processed batch of {} records", consumerType, records.size());

        } catch (Exception e) {
            log.error("[{}] Error handling DATA message", consumerType, e);
        }
    }

    @PreDestroy
    public void shutdown() {
        log.debug("[{}] Shutting down consumer connection...", consumerType);

        if (connection != null) {
            try {
                connection.disconnect();
                log.debug("[{}] Connection closed", consumerType);
            } catch (Exception e) {
                log.error("[{}] Error closing connection", consumerType, e);
            }
        }

        if (client != null) {
            try {
                client.shutdown();
                log.info("[{}] Client shutdown complete", consumerType);
            } catch (Exception e) {
                log.error("[{}] Error shutting down client", consumerType, e);
            }
        }
    }
}
