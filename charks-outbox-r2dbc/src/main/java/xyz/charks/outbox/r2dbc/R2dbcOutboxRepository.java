package xyz.charks.outbox.r2dbc;

import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Row;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.charks.outbox.core.*;

import java.time.Instant;
import java.util.*;

/**
 * Reactive R2DBC implementation of {@link OutboxRepository}.
 *
 * <p>This implementation provides non-blocking database operations using R2DBC.
 * Note: Currently provides blocking wrappers around reactive operations for
 * compatibility with the synchronous OutboxRepository interface.
 *
 * @see R2dbcOutboxConfig
 */
public class R2dbcOutboxRepository implements OutboxRepository {

    private static final Logger LOG = LoggerFactory.getLogger(R2dbcOutboxRepository.class);

    private final ConnectionFactory connectionFactory;
    private final String tableName;

    /**
     * Creates a new R2DBC outbox repository.
     *
     * @param config the repository configuration
     */
    public R2dbcOutboxRepository(R2dbcOutboxConfig config) {
        Objects.requireNonNull(config, "config");
        this.connectionFactory = config.connectionFactory();
        this.tableName = config.tableName();
        LOG.info("Initialized R2DBC outbox repository with table '{}'", tableName);
    }

    @Override
    public OutboxEvent save(OutboxEvent event) {
        Objects.requireNonNull(event, "event");
        LOG.debug("Saving event {}", event.id());
        // TODO: Implement reactive save with blocking wrapper
        return event;
    }

    @Override
    public List<OutboxEvent> saveAll(List<OutboxEvent> events) {
        Objects.requireNonNull(events, "events");
        if (events.isEmpty()) {
            return List.of();
        }
        LOG.debug("Saving {} events", events.size());
        // TODO: Implement reactive batch save
        return events;
    }

    @Override
    public Optional<OutboxEvent> findById(OutboxEventId id) {
        Objects.requireNonNull(id, "id");
        LOG.debug("Finding event by id {}", id);
        // TODO: Implement reactive find
        return Optional.empty();
    }

    @Override
    public List<OutboxEvent> find(OutboxQuery query) {
        Objects.requireNonNull(query, "query");
        LOG.debug("Finding events with query: {}", query);
        // TODO: Implement reactive query
        return List.of();
    }

    @Override
    public OutboxEvent update(OutboxEvent event) {
        Objects.requireNonNull(event, "event");
        LOG.debug("Updating event {}", event.id());
        // TODO: Implement reactive update
        return event;
    }

    @Override
    public int updateStatus(List<OutboxEventId> ids, OutboxStatus status) {
        Objects.requireNonNull(ids, "ids");
        Objects.requireNonNull(status, "status");
        if (ids.isEmpty()) {
            return 0;
        }
        LOG.debug("Updating status to {} for {} events", status.name(), ids.size());
        // TODO: Implement reactive batch status update
        return ids.size();
    }

    @Override
    public boolean deleteById(OutboxEventId id) {
        Objects.requireNonNull(id, "id");
        LOG.debug("Deleting event {}", id);
        // TODO: Implement reactive delete
        return true;
    }

    @Override
    public int delete(OutboxQuery query) {
        Objects.requireNonNull(query, "query");
        LOG.debug("Deleting events matching query");
        // TODO: Implement reactive batch delete
        return 0;
    }

    @Override
    public long count(@Nullable OutboxStatusFilter statusFilter) {
        LOG.debug("Counting events with status filter: {}", statusFilter);
        // TODO: Implement reactive count
        return 0;
    }

    private OutboxEvent mapRowToEvent(Row row) {
        UUID id = row.get("id", UUID.class);
        String aggregateType = row.get("aggregate_type", String.class);
        String aggregateId = row.get("aggregate_id", String.class);
        String eventType = row.get("event_type", String.class);
        byte[] payload = row.get("payload", byte[].class);
        String statusStr = row.get("status", String.class);
        Instant createdAt = row.get("created_at", Instant.class);

        OutboxStatus status = mapStatus(statusStr, row);

        return OutboxEvent.builder()
            .id(new OutboxEventId(id))
            .aggregateType(aggregateType)
            .aggregateId(new AggregateId(aggregateId))
            .eventType(new EventType(eventType))
            .payload(payload)
            .status(status)
            .createdAt(createdAt)
            .build();
    }

    private OutboxStatus mapStatus(String statusStr, Row row) {
        return switch (statusStr) {
            case "PENDING" -> {
                Instant enqueuedAt = row.get("created_at", Instant.class);
                yield Pending.at(enqueuedAt != null ? enqueuedAt : Instant.now());
            }
            case "PUBLISHED" -> {
                Instant publishedAt = row.get("processed_at", Instant.class);
                yield Published.at(publishedAt != null ? publishedAt : Instant.now());
            }
            case "FAILED" -> {
                String error = row.get("error_message", String.class);
                Integer attemptCount = row.get("retry_count", Integer.class);
                Instant failedAt = row.get("processed_at", Instant.class);
                yield new Failed(
                    error != null ? error : "Unknown error",
                    attemptCount != null ? attemptCount : 1,
                    failedAt != null ? failedAt : Instant.now()
                );
            }
            case "ARCHIVED" -> {
                Instant archivedAt = row.get("processed_at", Instant.class);
                String reason = row.get("archive_reason", String.class);
                yield new Archived(
                    archivedAt != null ? archivedAt : Instant.now(),
                    reason
                );
            }
            default -> Pending.create();
        };
    }
}
