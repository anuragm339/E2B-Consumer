package com.example.consumer.service;

import com.example.consumer.GenericConsumerHandler;
import com.messaging.common.model.BrokerMessage;
import com.messaging.network.tcp.NettyTcpClient;
import io.micronaut.context.annotation.Value;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.runtime.server.event.ServerStartupEvent;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
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

    @Value("${consumer.topic}")
    private String consumerTopic;

    @Value("${consumer.group}")
    private String consumerGroup;

    @Value("${consumer.type}")
    private String consumerType;

    @Inject
    private GenericConsumerHandler messageHandler;

    @Inject
    private ControlMessageHandler controlHandler;

    private NettyTcpClient client;
    private NettyTcpClient.Connection connection;

    @Override
    public void onApplicationEvent(ServerStartupEvent event) {
        log.info("[{}] Connecting to broker at {}:{}...", consumerType, brokerHost, brokerPort);

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
            log.info("[{}] Subscribing to topic: {}, group: {}", consumerType, consumerTopic, consumerGroup);

            String payload = String.format("{\"topic\":\"%s\",\"group\":\"%s\"}",
                                          consumerTopic, consumerGroup);

            BrokerMessage subscribeMsg = new BrokerMessage(
                BrokerMessage.MessageType.SUBSCRIBE,
                System.currentTimeMillis(),
                payload.getBytes(StandardCharsets.UTF_8)
            );

            connection.send(subscribeMsg).whenComplete((v, ex) -> {
                if (ex != null) {
                    log.error("[{}] Failed to send SUBSCRIBE", consumerType, ex);
                } else {
                    log.info("[{}] SUBSCRIBE sent successfully", consumerType);
                }
            });

        } catch (Exception e) {
            log.error("[{}] Error subscribing to topic", consumerType, e);
        }
    }

    private void handleMessage(BrokerMessage message) {
        try {
            log.debug("[{}] Received message: type={}, id={}",
                     consumerType, message.getType(), message.getMessageId());

            switch (message.getType()) {
                case DATA:
                    // TODO: Parse and handle data message
                    log.info("[{}] Received DATA message", consumerType);
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

    @PreDestroy
    public void shutdown() {
        log.info("[{}] Shutting down consumer connection...", consumerType);

        if (connection != null) {
            try {
                connection.disconnect();
                log.info("[{}] Connection closed", consumerType);
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
