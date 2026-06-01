package com.example.consumer.store;

import io.micronaut.context.annotation.Value;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.*;
import java.time.Instant;
import java.util.List;

/**
 * Stores every msgKey received from the broker into a local SQLite database.
 * Enables three-way verification: events.db (broker source) vs received_messages.db
 * (what consumer got) vs RocksDB ack-store (what broker thinks was ACKed).
 *
 * Uses INSERT OR IGNORE so replayed keys (after RESET) are not double-counted.
 */
@Singleton
public class ReceivedMessageStore {

    private static final Logger log = LoggerFactory.getLogger(ReceivedMessageStore.class);

    private static final String CREATE_TABLE = """
            CREATE TABLE IF NOT EXISTS received_messages (
                topic          TEXT NOT NULL,
                consumer_group TEXT NOT NULL,
                msg_key        TEXT NOT NULL,
                event_type     TEXT,
                created_at     TEXT,
                received_at    TEXT NOT NULL,
                PRIMARY KEY (topic, consumer_group, msg_key)
            )
            """;

    private static final String INSERT = """
            INSERT OR IGNORE INTO received_messages
                (topic, consumer_group, msg_key, event_type, created_at, received_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """;

    private final String dbPath;
    private Connection connection;

    public ReceivedMessageStore(
            @Value("${consumer.received-store.db-path:/app/consumer-data/received-messages.db}") String dbPath) {
        this.dbPath = dbPath;
    }

    @PostConstruct
    void init() throws SQLException {
        new File(dbPath).getParentFile().mkdirs();
        connection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
        try (Statement st = connection.createStatement()) {
            st.execute(CREATE_TABLE);
        }
        log.info("ReceivedMessageStore initialized: {}", dbPath);
    }

    /**
     * Persists a batch of received msgKeys for the given topic and group.
     * Called from GenericConsumerHandler.handleBatch() on every delivery.
     */
    public synchronized void storeBatch(String topic, String group,
                                        List<String> msgKeys,
                                        List<String> eventTypes,
                                        List<Instant> createdAts) {
        if (msgKeys.isEmpty()) return;
        String now = Instant.now().toString();
        try (PreparedStatement ps = connection.prepareStatement(INSERT)) {
            for (int i = 0; i < msgKeys.size(); i++) {
                String key = msgKeys.get(i);
                if (key == null) continue;
                ps.setString(1, topic);
                ps.setString(2, group);
                ps.setString(3, key);
                ps.setString(4, eventTypes != null ? eventTypes.get(i) : null);
                ps.setString(5, createdAts != null && createdAts.get(i) != null
                        ? createdAts.get(i).toString() : null);
                ps.setString(6, now);
                ps.addBatch();
            }
            ps.executeBatch();
        } catch (SQLException e) {
            log.warn("ReceivedMessageStore: batch insert failed for topic={} group={}: {}",
                    topic, group, e.getMessage());
        }
    }
}
