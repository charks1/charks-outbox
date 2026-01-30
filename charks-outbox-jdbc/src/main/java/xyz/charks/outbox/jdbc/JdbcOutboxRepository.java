package xyz.charks.outbox.jdbc;

import xyz.charks.outbox.core.AggregateId;
import xyz.charks.outbox.core.Archived;
import xyz.charks.outbox.core.EventType;
import xyz.charks.outbox.core.Failed;
import xyz.charks.outbox.core.HeadersCodec;
import xyz.charks.outbox.core.OutboxEvent;
import xyz.charks.outbox.core.OutboxEventId;
import xyz.charks.outbox.core.OutboxQuery;
import xyz.charks.outbox.core.OutboxStatusFilter;
import xyz.charks.outbox.core.OutboxRepository;
import xyz.charks.outbox.core.OutboxStatus;
import xyz.charks.outbox.core.Pending;
import xyz.charks.outbox.core.Published;
import xyz.charks.outbox.exception.OutboxPersistenceException;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.UUID;

/**
 * JDBC-based implementation of {@link OutboxRepository}.
 *
 * <p>This implementation supports PostgreSQL, MySQL, and H2 databases through
 * the {@link SqlDialect} abstraction. Queries use database-specific locking
 * mechanisms to support concurrent relay instances.
 *
 * <p>Example setup:
 * <pre>{@code
 * DataSource dataSource = HikariDataSourceBuilder.create()
 *     .jdbcUrl("jdbc:postgresql://localhost:5432/mydb")
 *     .username("user")
 *     .password("pass")
 *     .build();
 *
 * JdbcOutboxConfig config = JdbcOutboxConfig.postgresql();
 * OutboxRepository repository = new JdbcOutboxRepository(dataSource, config);
 * }</pre>
 *
 * <p>Transaction integration:
 * <pre>{@code
 * // Use with Spring @Transactional or manual transaction management
 * @Transactional
 * public void createOrder(Order order) {
 *     orderRepository.save(order);
 *     outboxRepository.save(OutboxEvent.builder()
 *         .aggregateType("Order")
 *         .aggregateId(order.getId())
 *         .eventType("OrderCreated")
 *         .topic("orders")
 *         .payload(serializer.serialize(event))
 *         .build());
 * }
 * }</pre>
 *
 * @see OutboxRepository
 * @see JdbcOutboxConfig
 * @see SqlDialect
 */
public final class JdbcOutboxRepository implements OutboxRepository {

    private static final Logger log = LoggerFactory.getLogger(JdbcOutboxRepository.class);

    private final DataSource dataSource;
    private final JdbcOutboxConfig config;
    private final String tableName;
    private final SqlDialect dialect;

    /**
     * Creates a new JdbcOutboxRepository with the specified configuration.
     *
     * @param dataSource the JDBC data source for database connections
     * @param config the repository configuration
     * @throws NullPointerException if dataSource or config is null
     */
    public JdbcOutboxRepository(DataSource dataSource, JdbcOutboxConfig config) {
        this.dataSource = Objects.requireNonNull(dataSource, "DataSource cannot be null");
        this.config = Objects.requireNonNull(config, "Config cannot be null");
        this.tableName = config.tableName();
        this.dialect = config.dialect();
    }

    /**
     * Creates a new JdbcOutboxRepository with default configuration for PostgreSQL.
     *
     * @param dataSource the JDBC data source
     * @throws NullPointerException if dataSource is null
     */
    public JdbcOutboxRepository(DataSource dataSource) {
        this(dataSource, JdbcOutboxConfig.defaults());
    }

    @Override
    public OutboxEvent save(OutboxEvent event) {
        Objects.requireNonNull(event, "Event cannot be null");

        String sql = """
                INSERT INTO %s (
                    id, aggregate_type, aggregate_id, event_type, topic,
                    partition_key, payload, headers, created_at, status,
                    retry_count, last_error, processed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, %s, ?, ?, ?, ?, ?)
                """.formatted(tableName, dialect.jsonPlaceholder());

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            setEventParameters(stmt, event);
            stmt.executeUpdate();

            log.debug("Saved outbox event: {}", event.id());
            return event;

        } catch (SQLException e) {
            throw new OutboxPersistenceException("Failed to save outbox event: " + event.id(), e);
        }
    }

    @Override
    public List<OutboxEvent> saveAll(List<OutboxEvent> events) {
        Objects.requireNonNull(events, "Events cannot be null");
        if (events.isEmpty()) {
            return List.of();
        }

        String sql = """
                INSERT INTO %s (
                    id, aggregate_type, aggregate_id, event_type, topic,
                    partition_key, payload, headers, created_at, status,
                    retry_count, last_error, processed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, %s, ?, ?, ?, ?, ?)
                """.formatted(tableName, dialect.jsonPlaceholder());

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            for (OutboxEvent event : events) {
                setEventParameters(stmt, event);
                stmt.addBatch();
            }

            stmt.executeBatch();
            log.debug("Saved {} outbox events", events.size());
            return events;

        } catch (SQLException e) {
            throw new OutboxPersistenceException("Failed to save outbox events batch", e);
        }
    }

    @Override
    public Optional<OutboxEvent> findById(OutboxEventId id) {
        Objects.requireNonNull(id, "ID cannot be null");

        String sql = "SELECT * FROM %s WHERE id = ?".formatted(tableName);

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setObject(1, id.value());
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapResultSetToEvent(rs));
                }
                return Optional.empty();
            }

        } catch (SQLException e) {
            throw new OutboxPersistenceException("Failed to find outbox event: " + id, e);
        }
    }

    @Override
    public List<OutboxEvent> find(OutboxQuery query) {
        Objects.requireNonNull(query, "Query cannot be null");

        QueryBuilder qb = buildSelectQuery(query);
        String sql = qb.sql() + dialect.lockClause(query.lockMode());

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            qb.setParameters(stmt);
            try (ResultSet rs = stmt.executeQuery()) {
                List<OutboxEvent> events = new ArrayList<>();
                while (rs.next()) {
                    events.add(mapResultSetToEvent(rs));
                }
                return events;
            }

        } catch (SQLException e) {
            throw new OutboxPersistenceException("Failed to find outbox events", e);
        }
    }

    @Override
    public OutboxEvent update(OutboxEvent event) {
        Objects.requireNonNull(event, "Event cannot be null");

        String sql = """
                UPDATE %s SET
                    aggregate_type = ?, aggregate_id = ?, event_type = ?,
                    topic = ?, partition_key = ?, payload = ?, headers = %s,
                    created_at = ?, status = ?, retry_count = ?,
                    last_error = ?, processed_at = ?
                WHERE id = ?
                """.formatted(tableName, dialect.jsonPlaceholder());

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            int idx = 1;
            stmt.setString(idx++, event.aggregateType());
            stmt.setString(idx++, event.aggregateId().value());
            stmt.setString(idx++, event.eventType().value());
            stmt.setString(idx++, event.topic());
            setNullableString(stmt, idx++, event.partitionKey());
            stmt.setBytes(idx++, event.payload());
            stmt.setString(idx++, HeadersCodec.serialize(event.headers()));
            stmt.setTimestamp(idx++, Timestamp.from(event.createdAt()));
            stmt.setString(idx++, event.status().name());
            stmt.setInt(idx++, event.retryCount());
            setNullableString(stmt, idx++, event.lastError());
            setNullableTimestamp(stmt, idx++, event.processedAt());
            stmt.setObject(idx, event.id().value());

            int rowsUpdated = stmt.executeUpdate();
            if (rowsUpdated == 0) {
                throw new OutboxPersistenceException("Event not found: " + event.id());
            }

            log.debug("Updated outbox event: {}", event.id());
            return event;

        } catch (SQLException e) {
            throw new OutboxPersistenceException("Failed to update outbox event: " + event.id(), e);
        }
    }

    @Override
    public int updateStatus(List<OutboxEventId> ids, OutboxStatus status) {
        Objects.requireNonNull(ids, "IDs cannot be null");
        Objects.requireNonNull(status, "Status cannot be null");
        if (ids.isEmpty()) {
            return 0;
        }

        String placeholders = String.join(",", ids.stream().map(_ -> "?").toList());
        String sql;
        int extraParams;

        switch (status) {
            case Failed f -> {
                sql = "UPDATE %s SET status = ?, last_error = ?, retry_count = ?, processed_at = ? WHERE id IN (%s)"
                        .formatted(tableName, placeholders);
                extraParams = 4;
            }
            case Published _ -> {
                sql = "UPDATE %s SET status = ?, processed_at = ? WHERE id IN (%s)"
                        .formatted(tableName, placeholders);
                extraParams = 2;
            }
            case Archived _ -> {
                sql = "UPDATE %s SET status = ?, processed_at = ?, last_error = ? WHERE id IN (%s)"
                        .formatted(tableName, placeholders);
                extraParams = 3;
            }
            default -> {
                sql = "UPDATE %s SET status = ? WHERE id IN (%s)".formatted(tableName, placeholders);
                extraParams = 1;
            }
        }

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            int idx = 1;
            switch (status) {
                case Failed f -> {
                    stmt.setString(idx++, status.name());
                    stmt.setString(idx++, f.error());
                    stmt.setInt(idx++, f.attemptCount());
                    stmt.setTimestamp(idx++, Timestamp.from(f.failedAt()));
                }
                case Published p -> {
                    stmt.setString(idx++, status.name());
                    stmt.setTimestamp(idx++, Timestamp.from(p.publishedAt()));
                }
                case Archived a -> {
                    stmt.setString(idx++, status.name());
                    stmt.setTimestamp(idx++, Timestamp.from(a.archivedAt()));
                    setNullableString(stmt, idx++, a.reason());
                }
                default -> stmt.setString(idx++, status.name());
            }

            for (OutboxEventId id : ids) {
                stmt.setObject(idx++, id.value());
            }

            int rowsUpdated = stmt.executeUpdate();
            log.debug("Updated status to {} for {} events", status.name(), rowsUpdated);
            return rowsUpdated;

        } catch (SQLException e) {
            throw new OutboxPersistenceException("Failed to update status for events", e);
        }
    }

    @Override
    public boolean deleteById(OutboxEventId id) {
        Objects.requireNonNull(id, "ID cannot be null");

        String sql = "DELETE FROM %s WHERE id = ?".formatted(tableName);

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setObject(1, id.value());
            int rowsDeleted = stmt.executeUpdate();

            if (rowsDeleted > 0) {
                log.debug("Deleted outbox event: {}", id);
                return true;
            }
            return false;

        } catch (SQLException e) {
            throw new OutboxPersistenceException("Failed to delete outbox event: " + id, e);
        }
    }

    @Override
    public int delete(OutboxQuery query) {
        Objects.requireNonNull(query, "Query cannot be null");

        QueryBuilder qb = buildDeleteQuery(query);

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(qb.sql())) {

            qb.setParameters(stmt);
            int rowsDeleted = stmt.executeUpdate();
            log.debug("Deleted {} outbox events", rowsDeleted);
            return rowsDeleted;

        } catch (SQLException e) {
            throw new OutboxPersistenceException("Failed to delete outbox events", e);
        }
    }

    @Override
    public long count(@Nullable OutboxStatusFilter statusFilter) {
        String sql;
        if (statusFilter == null) {
            sql = "SELECT COUNT(*) FROM %s".formatted(tableName);
        } else {
            sql = "SELECT COUNT(*) FROM %s WHERE %s".formatted(tableName, statusWhereClause(statusFilter));
        }

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            if (rs.next()) {
                return rs.getLong(1);
            }
            return 0;

        } catch (SQLException e) {
            throw new OutboxPersistenceException("Failed to count outbox events", e);
        }
    }

    /**
     * Returns the configuration used by this repository.
     *
     * @return the repository configuration
     */
    public JdbcOutboxConfig config() {
        return config;
    }

    private void setEventParameters(PreparedStatement stmt, OutboxEvent event) throws SQLException {
        int idx = 1;
        stmt.setObject(idx++, event.id().value());
        stmt.setString(idx++, event.aggregateType());
        stmt.setString(idx++, event.aggregateId().value());
        stmt.setString(idx++, event.eventType().value());
        stmt.setString(idx++, event.topic());
        setNullableString(stmt, idx++, event.partitionKey());
        stmt.setBytes(idx++, event.payload());
        stmt.setString(idx++, HeadersCodec.serialize(event.headers()));
        stmt.setTimestamp(idx++, Timestamp.from(event.createdAt()));
        stmt.setString(idx++, event.status().name());
        stmt.setInt(idx++, event.retryCount());
        setNullableString(stmt, idx++, event.lastError());
        setNullableTimestamp(stmt, idx, event.processedAt());
    }

    private OutboxEvent mapResultSetToEvent(ResultSet rs) throws SQLException {
        return OutboxEvent.builder()
                .id(new OutboxEventId(rs.getObject("id", UUID.class)))
                .aggregateType(rs.getString("aggregate_type"))
                .aggregateId(new AggregateId(rs.getString("aggregate_id")))
                .eventType(new EventType(rs.getString("event_type")))
                .topic(rs.getString("topic"))
                .partitionKey(rs.getString("partition_key"))
                .payload(rs.getBytes("payload"))
                .headers(HeadersCodec.deserialize(rs.getString("headers")))
                .createdAt(rs.getTimestamp("created_at").toInstant())
                .status(mapStatus(rs.getString("status"), rs))
                .retryCount(rs.getInt("retry_count"))
                .lastError(rs.getString("last_error"))
                .processedAt(mapNullableTimestamp(rs.getTimestamp("processed_at")))
                .build();
    }

    private OutboxStatus mapStatus(String statusName, ResultSet rs) throws SQLException {
        Instant createdAt = rs.getTimestamp("created_at").toInstant();
        Timestamp processedAt = rs.getTimestamp("processed_at");
        String lastError = rs.getString("last_error");
        int retryCount = rs.getInt("retry_count");

        return switch (statusName) {
            case "PENDING" -> new Pending(createdAt);
            case "PUBLISHED" -> new Published(processedAt != null ? processedAt.toInstant() : createdAt);
            case "FAILED" -> new Failed(
                    lastError != null ? lastError : "Unknown error",
                    retryCount,
                    processedAt != null ? processedAt.toInstant() : Instant.now()
            );
            case "ARCHIVED" -> new Archived(
                    processedAt != null ? processedAt.toInstant() : Instant.now(),
                    lastError
            );
            default -> new Pending(createdAt);
        };
    }

    private QueryBuilder buildSelectQuery(OutboxQuery query) {
        StringBuilder sql = new StringBuilder("SELECT * FROM ").append(tableName);
        List<Object> params = new ArrayList<>();
        StringJoiner where = new StringJoiner(" AND ");

        addWhereConditions(query, where, params);

        if (query.maxRetryCount() != null) {
            where.add("retry_count <= ?");
            params.add(query.maxRetryCount());
        }

        if (where.length() > 0) {
            sql.append(" WHERE ").append(where);
        }

        sql.append(" ORDER BY created_at ASC");
        sql.append(dialect.limitClause(query.limit()));

        return new QueryBuilder(sql.toString(), params);
    }

    private QueryBuilder buildDeleteQuery(OutboxQuery query) {
        StringBuilder sql = new StringBuilder("DELETE FROM ").append(tableName);
        List<Object> params = new ArrayList<>();
        StringJoiner where = new StringJoiner(" AND ");

        addWhereConditions(query, where, params);

        if (where.length() > 0) {
            sql.append(" WHERE ").append(where);
        }

        return new QueryBuilder(sql.toString(), params);
    }

    private void addWhereConditions(OutboxQuery query, StringJoiner where, List<Object> params) {
        if (query.statusFilter() != null) {
            where.add(statusWhereClause(query.statusFilter()));
        }

        if (!query.aggregateTypes().isEmpty()) {
            String placeholders = String.join(",", query.aggregateTypes().stream().map(_ -> "?").toList());
            where.add("aggregate_type IN (" + placeholders + ")");
            params.addAll(query.aggregateTypes());
        }

        if (!query.topics().isEmpty()) {
            String placeholders = String.join(",", query.topics().stream().map(_ -> "?").toList());
            where.add("topic IN (" + placeholders + ")");
            params.addAll(query.topics());
        }

        if (query.createdAfter() != null) {
            where.add("created_at >= ?");
            params.add(Timestamp.from(query.createdAfter()));
        }

        if (query.createdBefore() != null) {
            where.add("created_at < ?");
            params.add(Timestamp.from(query.createdBefore()));
        }
    }

    private String statusWhereClause(OutboxStatusFilter filter) {
        return switch (filter) {
            case PENDING -> "status = 'PENDING'";
            case PUBLISHED -> "status = 'PUBLISHED'";
            case FAILED -> "status = 'FAILED'";
            case ARCHIVED -> "status = 'ARCHIVED'";
            case RETRYABLE -> "status IN ('PENDING', 'FAILED')";
        };
    }

    private void setNullableString(PreparedStatement stmt, int index, @Nullable String value) throws SQLException {
        if (value == null) {
            stmt.setNull(index, Types.VARCHAR);
        } else {
            stmt.setString(index, value);
        }
    }

    private void setNullableTimestamp(PreparedStatement stmt, int index, @Nullable Instant value) throws SQLException {
        if (value == null) {
            stmt.setNull(index, Types.TIMESTAMP);
        } else {
            stmt.setTimestamp(index, Timestamp.from(value));
        }
    }

    private @Nullable Instant mapNullableTimestamp(@Nullable Timestamp ts) {
        return ts != null ? ts.toInstant() : null;
    }

    private record QueryBuilder(String sql, List<Object> params) {
        void setParameters(PreparedStatement stmt) throws SQLException {
            for (int i = 0; i < params.size(); i++) {
                Object param = params.get(i);
                if (param instanceof String s) {
                    stmt.setString(i + 1, s);
                } else if (param instanceof Timestamp ts) {
                    stmt.setTimestamp(i + 1, ts);
                } else if (param instanceof Integer n) {
                    stmt.setInt(i + 1, n);
                } else if (param instanceof UUID uuid) {
                    stmt.setObject(i + 1, uuid);
                } else {
                    stmt.setObject(i + 1, param);
                }
            }
        }
    }
}
