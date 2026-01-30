package xyz.charks.outbox.r2dbc;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Statement;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.charks.outbox.core.AggregateId;
import xyz.charks.outbox.core.Archived;
import xyz.charks.outbox.core.EventType;
import xyz.charks.outbox.core.Failed;
import xyz.charks.outbox.core.HeadersCodec;
import xyz.charks.outbox.core.OutboxEvent;
import xyz.charks.outbox.core.OutboxEventId;
import xyz.charks.outbox.core.OutboxQuery;
import xyz.charks.outbox.core.OutboxRepository;
import xyz.charks.outbox.core.OutboxStatus;
import xyz.charks.outbox.core.OutboxStatusFilter;
import xyz.charks.outbox.core.Pending;
import xyz.charks.outbox.core.Published;
import xyz.charks.outbox.exception.OutboxPersistenceException;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Reactive R2DBC implementation of {@link OutboxRepository}.
 *
 * <p>This implementation provides non-blocking database operations using R2DBC.
 * Methods block internally to satisfy the synchronous OutboxRepository interface.
 *
 * @see R2dbcOutboxConfig
 */
public class R2dbcOutboxRepository implements OutboxRepository {

    private static final Logger LOG = LoggerFactory.getLogger(R2dbcOutboxRepository.class);
    private static final String STATUS_PENDING = "PENDING";
    private static final String STATUS_FAILED = "FAILED";

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

        String sql = """
                INSERT INTO %s (
                    id, aggregate_type, aggregate_id, event_type, topic,
                    partition_key, payload, headers, created_at, status,
                    retry_count, last_error, processed_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                """.formatted(tableName);

        Mono.usingWhen(
                connectionFactory.create(),
                conn -> executeInsert(conn, sql, event),
                conn -> Mono.from(conn.close())
        ).block();

        return event;
    }

    private Mono<Long> executeInsert(Connection conn, String sql, OutboxEvent event) {
        Statement stmt = conn.createStatement(sql)
                .bind("$1", event.id().value())
                .bind("$2", event.aggregateType())
                .bind("$3", event.aggregateId().value())
                .bind("$4", event.eventType().value())
                .bind("$5", event.topic());

        if (event.partitionKey() != null) {
            stmt.bind("$6", event.partitionKey());
        } else {
            stmt.bindNull("$6", String.class);
        }

        stmt.bind("$7", event.payload())
                .bind("$8", HeadersCodec.serialize(event.headers()))
                .bind("$9", event.createdAt())
                .bind("$10", event.status().name())
                .bind("$11", event.retryCount());

        if (event.lastError() != null) {
            stmt.bind("$12", event.lastError());
        } else {
            stmt.bindNull("$12", String.class);
        }

        if (event.processedAt() != null) {
            stmt.bind("$13", event.processedAt());
        } else {
            stmt.bindNull("$13", Instant.class);
        }

        return Flux.from(stmt.execute())
                .flatMap(Result::getRowsUpdated)
                .reduce(Long::sum);
    }

    @Override
    public List<OutboxEvent> saveAll(List<OutboxEvent> events) {
        Objects.requireNonNull(events, "events");
        if (events.isEmpty()) {
            return List.of();
        }
        LOG.debug("Saving {} events", events.size());

        for (OutboxEvent event : events) {
            save(event);
        }
        return events;
    }

    @Override
    public Optional<OutboxEvent> findById(OutboxEventId id) {
        Objects.requireNonNull(id, "id");
        LOG.debug("Finding event by id {}", id);

        String sql = "SELECT * FROM %s WHERE id = $1".formatted(tableName);

        return Mono.usingWhen(
                connectionFactory.create(),
                conn -> executeFindById(conn, sql, id),
                conn -> Mono.from(conn.close())
        ).blockOptional();
    }

    private Mono<OutboxEvent> executeFindById(Connection conn, String sql, OutboxEventId id) {
        Statement stmt = conn.createStatement(sql)
                .bind("$1", id.value());

        return Flux.from(stmt.execute())
                .flatMap(result -> result.map(this::mapRowToEvent))
                .singleOrEmpty();
    }

    @Override
    public List<OutboxEvent> find(OutboxQuery query) {
        Objects.requireNonNull(query, "query");
        LOG.debug("Finding events with query: {}", query);

        StringBuilder sql = new StringBuilder("SELECT * FROM ").append(tableName);
        List<Object> params = new ArrayList<>();
        int paramIndex = 1;

        List<String> conditions = new ArrayList<>();

        if (query.statusFilter() != null) {
            if (query.statusFilter() == OutboxStatusFilter.RETRYABLE) {
                conditions.add("status IN ($" + paramIndex++ + ", $" + paramIndex++ + ")");
                params.add(STATUS_PENDING);
                params.add(STATUS_FAILED);
            } else {
                conditions.add("status = $" + paramIndex++);
                params.add(mapStatusFilter(query.statusFilter()));
            }
        }

        if (!query.aggregateTypes().isEmpty()) {
            List<String> placeholderList = new ArrayList<>();
            for (String type : query.aggregateTypes()) {
                placeholderList.add("$" + paramIndex++);
                params.add(type);
            }
            conditions.add("aggregate_type IN (" + String.join(", ", placeholderList) + ")");
        }

        if (!conditions.isEmpty()) {
            sql.append(" WHERE ").append(String.join(" AND ", conditions));
        }

        sql.append(" ORDER BY created_at ASC");
        if (query.limit() > 0) {
            sql.append(" LIMIT ").append(query.limit());
        }

        String finalSql = sql.toString();

        return Flux.usingWhen(
                connectionFactory.create(),
                conn -> executeFind(conn, finalSql, params),
                conn -> Mono.from(conn.close())
        ).collectList().block();
    }

    private Flux<OutboxEvent> executeFind(Connection conn, String sql, List<Object> params) {
        Statement stmt = conn.createStatement(sql);
        for (int i = 0; i < params.size(); i++) {
            stmt.bind("$" + (i + 1), params.get(i));
        }

        return Flux.from(stmt.execute())
                .flatMap(result -> result.map(this::mapRowToEvent));
    }

    private String mapStatusFilter(OutboxStatusFilter filter) {
        return switch (filter) {
            case PENDING -> STATUS_PENDING;
            case PUBLISHED -> "PUBLISHED";
            case FAILED -> STATUS_FAILED;
            case ARCHIVED -> "ARCHIVED";
            case RETRYABLE -> STATUS_PENDING;
        };
    }

    @Override
    public OutboxEvent update(OutboxEvent event) {
        Objects.requireNonNull(event, "event");
        LOG.debug("Updating event {}", event.id());

        String sql = """
                UPDATE %s SET
                    aggregate_type = $1, aggregate_id = $2, event_type = $3,
                    topic = $4, partition_key = $5, payload = $6, headers = $7,
                    created_at = $8, status = $9, retry_count = $10,
                    last_error = $11, processed_at = $12
                WHERE id = $13
                """.formatted(tableName);

        Long rowsUpdated = Mono.usingWhen(
                connectionFactory.create(),
                conn -> executeUpdate(conn, sql, event),
                conn -> Mono.from(conn.close())
        ).block();

        if (rowsUpdated == null || rowsUpdated == 0) {
            throw new OutboxPersistenceException("Event not found: " + event.id());
        }

        return event;
    }

    private Mono<Long> executeUpdate(Connection conn, String sql, OutboxEvent event) {
        Statement stmt = conn.createStatement(sql)
                .bind("$1", event.aggregateType())
                .bind("$2", event.aggregateId().value())
                .bind("$3", event.eventType().value())
                .bind("$4", event.topic());

        if (event.partitionKey() != null) {
            stmt.bind("$5", event.partitionKey());
        } else {
            stmt.bindNull("$5", String.class);
        }

        stmt.bind("$6", event.payload())
                .bind("$7", HeadersCodec.serialize(event.headers()))
                .bind("$8", event.createdAt())
                .bind("$9", event.status().name())
                .bind("$10", event.retryCount());

        if (event.lastError() != null) {
            stmt.bind("$11", event.lastError());
        } else {
            stmt.bindNull("$11", String.class);
        }

        if (event.processedAt() != null) {
            stmt.bind("$12", event.processedAt());
        } else {
            stmt.bindNull("$12", Instant.class);
        }

        stmt.bind("$13", event.id().value());

        return Flux.from(stmt.execute())
                .flatMap(Result::getRowsUpdated)
                .reduce(Long::sum);
    }

    @Override
    public int updateStatus(List<OutboxEventId> ids, OutboxStatus status) {
        Objects.requireNonNull(ids, "ids");
        Objects.requireNonNull(status, "status");
        if (ids.isEmpty()) {
            return 0;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Updating status to {} for {} events", status.name(), ids.size());
        }

        int totalUpdated = 0;
        for (OutboxEventId id : ids) {
            Long updated = Mono.usingWhen(
                    connectionFactory.create(),
                    conn -> executeStatusUpdate(conn, status, id),
                    conn -> Mono.from(conn.close())
            ).block();
            if (updated != null) {
                totalUpdated += updated.intValue();
            }
        }
        return totalUpdated;
    }

    private Mono<Long> executeStatusUpdate(Connection conn, OutboxStatus status, OutboxEventId id) {
        String sql;
        Statement stmt;

        switch (status) {
            case Failed f -> {
                sql = "UPDATE %s SET status = $1, last_error = $2, retry_count = $3, processed_at = $4 WHERE id = $5"
                        .formatted(tableName);
                stmt = conn.createStatement(sql)
                        .bind("$1", status.name())
                        .bind("$2", f.error())
                        .bind("$3", f.attemptCount())
                        .bind("$4", f.failedAt())
                        .bind("$5", id.value());
            }
            case Published p -> {
                sql = "UPDATE %s SET status = $1, processed_at = $2 WHERE id = $3".formatted(tableName);
                stmt = conn.createStatement(sql)
                        .bind("$1", status.name())
                        .bind("$2", p.publishedAt())
                        .bind("$3", id.value());
            }
            case Archived a -> {
                sql = "UPDATE %s SET status = $1, processed_at = $2, last_error = $3 WHERE id = $4".formatted(tableName);
                stmt = conn.createStatement(sql)
                        .bind("$1", status.name())
                        .bind("$2", a.archivedAt());
                if (a.reason() != null) {
                    stmt.bind("$3", a.reason());
                } else {
                    stmt.bindNull("$3", String.class);
                }
                stmt.bind("$4", id.value());
            }
            default -> {
                sql = "UPDATE %s SET status = $1 WHERE id = $2".formatted(tableName);
                stmt = conn.createStatement(sql)
                        .bind("$1", status.name())
                        .bind("$2", id.value());
            }
        }

        return Flux.from(stmt.execute())
                .flatMap(Result::getRowsUpdated)
                .reduce(Long::sum);
    }

    @Override
    public boolean deleteById(OutboxEventId id) {
        Objects.requireNonNull(id, "id");
        LOG.debug("Deleting event {}", id);

        String sql = "DELETE FROM %s WHERE id = $1".formatted(tableName);

        Long rowsDeleted = Mono.usingWhen(
                connectionFactory.create(),
                conn -> executeDelete(conn, sql, id),
                conn -> Mono.from(conn.close())
        ).block();

        return rowsDeleted != null && rowsDeleted > 0;
    }

    private Mono<Long> executeDelete(Connection conn, String sql, OutboxEventId id) {
        Statement stmt = conn.createStatement(sql)
                .bind("$1", id.value());

        return Flux.from(stmt.execute())
                .flatMap(Result::getRowsUpdated)
                .reduce(Long::sum);
    }

    @Override
    public int delete(OutboxQuery query) {
        Objects.requireNonNull(query, "query");
        LOG.debug("Deleting events matching query");

        StringBuilder sql = new StringBuilder("DELETE FROM ").append(tableName);
        List<Object> params = new ArrayList<>();
        int paramIndex = 1;

        List<String> conditions = new ArrayList<>();

        if (query.statusFilter() != null) {
            if (query.statusFilter() == OutboxStatusFilter.RETRYABLE) {
                conditions.add("status IN ($" + paramIndex++ + ", $" + paramIndex++ + ")");
                params.add(STATUS_PENDING);
                params.add(STATUS_FAILED);
            } else {
                conditions.add("status = $" + paramIndex++);
                params.add(mapStatusFilter(query.statusFilter()));
            }
        }

        if (!conditions.isEmpty()) {
            sql.append(" WHERE ").append(String.join(" AND ", conditions));
        }

        String finalSql = sql.toString();

        Long rowsDeleted = Mono.usingWhen(
                connectionFactory.create(),
                conn -> executeDeleteQuery(conn, finalSql, params),
                conn -> Mono.from(conn.close())
        ).block();

        return rowsDeleted != null ? rowsDeleted.intValue() : 0;
    }

    private Mono<Long> executeDeleteQuery(Connection conn, String sql, List<Object> params) {
        Statement stmt = conn.createStatement(sql);
        for (int i = 0; i < params.size(); i++) {
            stmt.bind("$" + (i + 1), params.get(i));
        }

        return Flux.from(stmt.execute())
                .flatMap(Result::getRowsUpdated)
                .reduce(Long::sum);
    }

    @Override
    public long count(@Nullable OutboxStatusFilter statusFilter) {
        LOG.debug("Counting events with status filter: {}", statusFilter);

        String sql;
        List<Object> params = new ArrayList<>();

        if (statusFilter == null) {
            sql = "SELECT COUNT(*) FROM %s".formatted(tableName);
        } else if (statusFilter == OutboxStatusFilter.RETRYABLE) {
            sql = "SELECT COUNT(*) FROM %s WHERE status IN ($1, $2)".formatted(tableName);
            params.add(STATUS_PENDING);
            params.add(STATUS_FAILED);
        } else {
            sql = "SELECT COUNT(*) FROM %s WHERE status = $1".formatted(tableName);
            params.add(mapStatusFilter(statusFilter));
        }

        String finalSql = sql;

        Long count = Mono.usingWhen(
                connectionFactory.create(),
                conn -> executeCount(conn, finalSql, params),
                conn -> Mono.from(conn.close())
        ).block();

        return count != null ? count : 0;
    }

    private Mono<Long> executeCount(Connection conn, String sql, List<Object> params) {
        Statement stmt = conn.createStatement(sql);
        for (int i = 0; i < params.size(); i++) {
            stmt.bind("$" + (i + 1), params.get(i));
        }

        return Flux.from(stmt.execute())
                .flatMap(result -> result.map((row, meta) -> row.get(0, Long.class)))
                .singleOrEmpty();
    }

    private OutboxEvent mapRowToEvent(Row row, RowMetadata metadata) {
        UUID id = row.get("id", UUID.class);
        String aggregateType = row.get("aggregate_type", String.class);
        String aggregateId = row.get("aggregate_id", String.class);
        String eventType = row.get("event_type", String.class);
        String topic = row.get("topic", String.class);
        String partitionKey = row.get("partition_key", String.class);
        byte[] payload = row.get("payload", byte[].class);
        String headersJson = row.get("headers", String.class);
        Instant createdAt = row.get("created_at", Instant.class);
        String statusStr = row.get("status", String.class);
        Integer retryCount = row.get("retry_count", Integer.class);
        String lastError = row.get("last_error", String.class);
        Instant processedAt = row.get("processed_at", Instant.class);

        OutboxStatus status = mapStatus(statusStr, createdAt, processedAt, lastError, retryCount);

        return OutboxEvent.builder()
                .id(new OutboxEventId(id))
                .aggregateType(aggregateType)
                .aggregateId(new AggregateId(aggregateId))
                .eventType(new EventType(eventType))
                .topic(topic)
                .partitionKey(partitionKey)
                .payload(payload)
                .headers(HeadersCodec.deserialize(headersJson))
                .createdAt(createdAt)
                .status(status)
                .retryCount(retryCount != null ? retryCount : 0)
                .lastError(lastError)
                .processedAt(processedAt)
                .build();
    }

    private OutboxStatus mapStatus(String statusStr, Instant createdAt,
                                   @Nullable Instant processedAt,
                                   @Nullable String lastError,
                                   @Nullable Integer retryCount) {
        return switch (statusStr) {
            case "PENDING" -> Pending.at(createdAt != null ? createdAt : Instant.now());
            case "PUBLISHED" -> Published.at(processedAt != null ? processedAt : Instant.now());
            case "FAILED" -> new Failed(
                    lastError != null ? lastError : "Unknown error",
                    retryCount != null ? retryCount : 1,
                    processedAt != null ? processedAt : Instant.now()
            );
            case "ARCHIVED" -> new Archived(
                    processedAt != null ? processedAt : Instant.now(),
                    lastError
            );
            default -> Pending.create();
        };
    }
}
