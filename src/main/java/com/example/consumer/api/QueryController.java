package com.example.consumer.api;

import com.example.consumer.service.ConsumerStorageService;
import com.example.consumer.service.SegmentMetadataService;
import com.messaging.common.model.MessageRecord;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import jakarta.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * REST API for querying consumer data and metadata
 */
@Controller("/api/query")
public class QueryController {

    @Inject
    private ConsumerStorageService storageService;

    @Inject
    private SegmentMetadataService metadata;

    /**
     * Get message by offset (uses SQLite to find segment quickly)
     */
    @Get("/offset/{offset}")
    public HttpResponse<?> findByOffset(@PathVariable long offset) {
        try {
            MessageRecord record = storageService.findByOffset(offset);

            if (record == null) {
                return HttpResponse.notFound(Map.of("error", "No message found at offset " + offset));
            }

            return HttpResponse.ok(Map.of(
                "offset", offset,
                "msgKey", record.getMsgKey(),
                "eventType", record.getEventType().toString(),
                "data", record.getData() != null ? record.getData() : "null",
                "createdAt", record.getCreatedAt().toString()
            ));

        } catch (Exception e) {
            return HttpResponse.serverError(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Get metadata statistics (shows SQLite is tiny)
     */
    @Get("/stats")
    public HttpResponse<?> getStats() {
        try {
            SegmentMetadataService.SegmentStats stats = metadata.getStats();

            return HttpResponse.ok(Map.of(
                "segmentCount", stats.segmentCount(),
                "totalRecords", stats.totalRecords(),
                "offsetRange", Map.of(
                    "min", stats.minOffset(),
                    "max", stats.maxOffset()
                ),
                "sqliteDbSize", Map.of(
                    "bytes", stats.dbSizeBytes(),
                    "kilobytes", stats.dbSizeBytes() / 1024,
                    "megabytes", stats.dbSizeBytes() / (1024 * 1024)
                )
            ));

        } catch (Exception e) {
            return HttpResponse.serverError(Map.of("error", e.getMessage()));
        }
    }

    /**
     * List all segments with their ranges
     */
    @Get("/segments")
    public HttpResponse<?> listSegments() {
        try {
            List<SegmentMetadataService.SegmentInfo> segments = metadata.getAllSegments();

            return HttpResponse.ok(Map.of(
                "count", segments.size(),
                "segments", segments.stream().map(s -> Map.of(
                    "segmentId", s.segmentId(),
                    "file", s.segmentFile(),
                    "startOffset", s.startOffset(),
                    "endOffset", s.endOffset(),
                    "range", String.format("[%d, %d]", s.startOffset(), s.endOffset()),
                    "capacity", s.endOffset() - s.startOffset() + 1
                )).collect(Collectors.toList())
            ));

        } catch (Exception e) {
            return HttpResponse.serverError(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Health check endpoint
     */
    @Get("/health")
    public HttpResponse<?> health() {
        try {
            SegmentMetadataService.SegmentStats stats = metadata.getStats();

            return HttpResponse.ok(Map.of(
                "status", "UP",
                "segments", stats.segmentCount(),
                "records", stats.totalRecords()
            ));

        } catch (Exception e) {
            return HttpResponse.serverError(Map.of(
                "status", "DOWN",
                "error", e.getMessage()
            ));
        }
    }
}
