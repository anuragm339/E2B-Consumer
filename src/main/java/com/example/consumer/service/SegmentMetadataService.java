package com.example.consumer.service;

import io.micronaut.context.annotation.Value;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Minimal SQLite metadata service that ONLY tracks segment offset ranges.
 * Keeps SQLite size extremely small (KB, not MB/GB).
 */
@Singleton
public class SegmentMetadataService {
    private static final Logger log = LoggerFactory.getLogger(SegmentMetadataService.class);

    private final Connection connection;
    private final String consumerType;

    public SegmentMetadataService(
            @Value("${consumer.type}") String consumerType,
            @Value("${storage.data-dir}") String dataDir) throws SQLException {

        this.consumerType = consumerType;

        // Ensure data directory exists
        try {
            java.nio.file.Files.createDirectories(java.nio.file.Paths.get(dataDir));
        } catch (java.io.IOException e) {
            throw new SQLException("Failed to create data directory: " + dataDir, e);
        }

        // SQLite database file - will be VERY small
        String dbPath = dataDir + "/segment-metadata.db";
        this.connection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);

        initializeSchema();

        log.info("[{}] Segment metadata index initialized: {}", consumerType, dbPath);
    }

    private void initializeSchema() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            // Only track segment offset ranges
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS segment_ranges (
                    segment_id INTEGER PRIMARY KEY,
                    segment_file VARCHAR(255) NOT NULL,
                    start_offset BIGINT NOT NULL,
                    end_offset BIGINT NOT NULL,
                    record_count INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """);

            // Index for fast offset lookup
            stmt.execute("""
                CREATE INDEX IF NOT EXISTS idx_offset_lookup
                ON segment_ranges(start_offset, end_offset)
            """);

            log.info("[{}] SQLite schema initialized", consumerType);
        }
    }

    /**
     * Register a new segment with its offset range
     */
    public void registerSegment(int segmentId, String segmentFile,
                                long startOffset, long endOffset,
                                int recordCount) throws SQLException {

        String sql = """
            INSERT OR REPLACE INTO segment_ranges
            (segment_id, segment_file, start_offset, end_offset, record_count)
            VALUES (?, ?, ?, ?, ?)
        """;

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setInt(1, segmentId);
            pstmt.setString(2, segmentFile);
            pstmt.setLong(3, startOffset);
            pstmt.setLong(4, endOffset);
            pstmt.setInt(5, recordCount);
            pstmt.executeUpdate();
        }

        log.info("[{}] Registered segment: id={}, file={}, range=[{}, {}], records={}",
                 consumerType, segmentId, segmentFile, startOffset, endOffset, recordCount);
    }

    /**
     * Find which segment contains a specific offset (O(log n) with B-tree index)
     */
    public SegmentInfo findSegmentByOffset(long offset) throws SQLException {
        String sql = """
            SELECT segment_id, segment_file, start_offset, end_offset
            FROM segment_ranges
            WHERE start_offset <= ? AND end_offset >= ?
            LIMIT 1
        """;

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setLong(1, offset);
            pstmt.setLong(2, offset);

            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return new SegmentInfo(
                        rs.getInt("segment_id"),
                        rs.getString("segment_file"),
                        rs.getLong("start_offset"),
                        rs.getLong("end_offset")
                    );
                }
            }
        }

        log.debug("[{}] No segment found for offset: {}", consumerType, offset);
        return null;
    }

    /**
     * Get all segments (for full scan if needed)
     */
    public List<SegmentInfo> getAllSegments() throws SQLException {
        List<SegmentInfo> segments = new ArrayList<>();

        String sql = "SELECT segment_id, segment_file, start_offset, end_offset FROM segment_ranges ORDER BY segment_id";

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                segments.add(new SegmentInfo(
                    rs.getInt("segment_id"),
                    rs.getString("segment_file"),
                    rs.getLong("start_offset"),
                    rs.getLong("end_offset")
                ));
            }
        }

        return segments;
    }

    /**
     * Get current offset (highest end_offset)
     */
    public long getCurrentOffset() throws SQLException {
        String sql = "SELECT MAX(end_offset) as max_offset FROM segment_ranges";

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            if (rs.next()) {
                long maxOffset = rs.getLong("max_offset");
                return rs.wasNull() ? 0 : maxOffset;
            }
        }

        return 0;
    }

    /**
     * Clear all metadata (called on RESET)
     */
    public void clearAllMetadata() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DELETE FROM segment_ranges");
            stmt.execute("VACUUM"); // Reclaim space - keeps DB tiny
        }

        log.info("[{}] Cleared all segment metadata", consumerType);
    }

    /**
     * Get database size in bytes
     */
    public long getDatabaseSize() {
        try {
            String sql = "SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size()";
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                if (rs.next()) {
                    return rs.getLong("size");
                }
            }
        } catch (SQLException e) {
            log.warn("Failed to get DB size", e);
        }
        return 0;
    }

    /**
     * Get statistics
     */
    public SegmentStats getStats() throws SQLException {
        String sql = """
            SELECT
                COUNT(*) as segment_count,
                COALESCE(SUM(record_count), 0) as total_records,
                COALESCE(MIN(start_offset), 0) as min_offset,
                COALESCE(MAX(end_offset), 0) as max_offset
            FROM segment_ranges
        """;

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            if (rs.next()) {
                return new SegmentStats(
                    rs.getInt("segment_count"),
                    rs.getLong("total_records"),
                    rs.getLong("min_offset"),
                    rs.getLong("max_offset"),
                    getDatabaseSize()
                );
            }
        }

        return new SegmentStats(0, 0, 0, 0, 0);
    }

    @PreDestroy
    public void close() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    // Helper records
    public record SegmentInfo(int segmentId, String segmentFile, long startOffset, long endOffset) {}
    public record SegmentStats(int segmentCount, long totalRecords, long minOffset, long maxOffset, long dbSizeBytes) {}
}
