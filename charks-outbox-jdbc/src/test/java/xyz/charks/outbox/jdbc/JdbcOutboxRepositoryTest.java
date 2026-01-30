package xyz.charks.outbox.jdbc;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import xyz.charks.outbox.core.*;
import xyz.charks.outbox.exception.OutboxPersistenceException;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("JdbcOutboxRepository")
class JdbcOutboxRepositoryTest {

    @Mock
    private DataSource dataSource;

    @Mock
    private Connection connection;

    @Mock
    private PreparedStatement preparedStatement;

    @Mock
    private ResultSet resultSet;

    private JdbcOutboxRepository repository;

    @BeforeEach
    void setUp() {
        repository = new JdbcOutboxRepository(dataSource, JdbcOutboxConfig.postgresql());
    }

    @Nested
    @DisplayName("constructor")
    class ConstructorTest {

        @Test
        @DisplayName("creates repository with default config")
        void createsWithDefaultConfig() {
            JdbcOutboxRepository repo = new JdbcOutboxRepository(dataSource);

            assertThat(repo.config()).isNotNull();
            assertThat(repo.config().tableName()).isEqualTo("outbox_events");
        }

        @Test
        @DisplayName("throws exception for null dataSource")
        void nullDataSource() {
            assertThatThrownBy(() -> new JdbcOutboxRepository(null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("DataSource");
        }

        @Test
        @DisplayName("throws exception for null config")
        void nullConfig() {
            assertThatThrownBy(() -> new JdbcOutboxRepository(dataSource, null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("Config");
        }
    }

    @Nested
    @DisplayName("save")
    class SaveTest {

        @Test
        @DisplayName("throws exception for null event")
        void nullEvent() {
            assertThatThrownBy(() -> repository.save(null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("Event");
        }

        @Test
        @DisplayName("wraps SQLException in OutboxPersistenceException")
        void wrapsSqlException() throws SQLException {
            OutboxEvent event = createTestEvent();
            when(dataSource.getConnection()).thenThrow(new SQLException("Connection failed"));

            assertThatThrownBy(() -> repository.save(event))
                    .isInstanceOf(OutboxPersistenceException.class)
                    .hasMessageContaining("Failed to save")
                    .hasCauseInstanceOf(SQLException.class);
        }

        @Test
        @DisplayName("saves event and returns it")
        void savesEvent() throws SQLException {
            OutboxEvent event = createTestEvent();
            setupConnectionMocks();
            when(preparedStatement.executeUpdate()).thenReturn(1);

            OutboxEvent result = repository.save(event);

            assertThat(result).isEqualTo(event);
            verify(preparedStatement).executeUpdate();
        }
    }

    @Nested
    @DisplayName("saveAll")
    class SaveAllTest {

        @Test
        @DisplayName("throws exception for null list")
        void nullList() {
            assertThatThrownBy(() -> repository.saveAll(null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("Events");
        }

        @Test
        @DisplayName("returns empty list for empty input")
        void emptyInput() {
            List<OutboxEvent> result = repository.saveAll(List.of());

            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("wraps SQLException in OutboxPersistenceException")
        void wrapsSqlException() throws SQLException {
            when(dataSource.getConnection()).thenThrow(new SQLException("Connection failed"));

            assertThatThrownBy(() -> repository.saveAll(List.of(createTestEvent())))
                    .isInstanceOf(OutboxPersistenceException.class)
                    .hasMessageContaining("Failed to save")
                    .hasCauseInstanceOf(SQLException.class);
        }

        @Test
        @DisplayName("saves multiple events")
        void savesMultipleEvents() throws SQLException {
            setupConnectionMocks();
            when(preparedStatement.executeBatch()).thenReturn(new int[]{1, 1});

            List<OutboxEvent> events = List.of(createTestEvent(), createTestEvent());
            List<OutboxEvent> result = repository.saveAll(events);

            assertThat(result).hasSize(2);
            verify(preparedStatement).executeBatch();
        }
    }

    @Nested
    @DisplayName("findById")
    class FindByIdTest {

        @Test
        @DisplayName("throws exception for null id")
        void nullId() {
            assertThatThrownBy(() -> repository.findById(null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("ID");
        }

        @Test
        @DisplayName("wraps SQLException in OutboxPersistenceException")
        void wrapsSqlException() throws SQLException {
            when(dataSource.getConnection()).thenThrow(new SQLException("Connection failed"));

            assertThatThrownBy(() -> repository.findById(OutboxEventId.generate()))
                    .isInstanceOf(OutboxPersistenceException.class)
                    .hasMessageContaining("Failed to find")
                    .hasCauseInstanceOf(SQLException.class);
        }

        @Test
        @DisplayName("returns empty when not found")
        void returnsEmptyWhenNotFound() throws SQLException {
            setupConnectionMocks();
            when(preparedStatement.executeQuery()).thenReturn(resultSet);
            when(resultSet.next()).thenReturn(false);

            var result = repository.findById(OutboxEventId.generate());

            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("returns event with PENDING status")
        void returnsEventWithPendingStatus() throws SQLException {
            UUID eventId = UUID.randomUUID();
            Instant now = Instant.now();
            setupConnectionMocks();
            when(preparedStatement.executeQuery()).thenReturn(resultSet);
            when(resultSet.next()).thenReturn(true);
            setupResultSetMocks(eventId, "PENDING", now, null, null);

            var result = repository.findById(new OutboxEventId(eventId));

            assertThat(result).isPresent();
            assertThat(result.get().status()).isInstanceOf(xyz.charks.outbox.core.Pending.class);
        }

        @Test
        @DisplayName("returns event with PUBLISHED status")
        void returnsEventWithPublishedStatus() throws SQLException {
            UUID eventId = UUID.randomUUID();
            Instant now = Instant.now();
            setupConnectionMocks();
            when(preparedStatement.executeQuery()).thenReturn(resultSet);
            when(resultSet.next()).thenReturn(true);
            setupResultSetMocks(eventId, "PUBLISHED", now, now, null);

            var result = repository.findById(new OutboxEventId(eventId));

            assertThat(result).isPresent();
            assertThat(result.get().status()).isInstanceOf(Published.class);
        }

        @Test
        @DisplayName("returns event with FAILED status")
        void returnsEventWithFailedStatus() throws SQLException {
            UUID eventId = UUID.randomUUID();
            Instant now = Instant.now();
            setupConnectionMocks();
            when(preparedStatement.executeQuery()).thenReturn(resultSet);
            when(resultSet.next()).thenReturn(true);
            setupResultSetMocks(eventId, "FAILED", now, now, "Connection timeout");

            var result = repository.findById(new OutboxEventId(eventId));

            assertThat(result).isPresent();
            assertThat(result.get().status()).isInstanceOf(xyz.charks.outbox.core.Failed.class);
            assertThat(result.get().lastError()).isEqualTo("Connection timeout");
        }

        @Test
        @DisplayName("returns event with ARCHIVED status")
        void returnsEventWithArchivedStatus() throws SQLException {
            UUID eventId = UUID.randomUUID();
            Instant now = Instant.now();
            setupConnectionMocks();
            when(preparedStatement.executeQuery()).thenReturn(resultSet);
            when(resultSet.next()).thenReturn(true);
            setupResultSetMocks(eventId, "ARCHIVED", now, now, null);

            var result = repository.findById(new OutboxEventId(eventId));

            assertThat(result).isPresent();
            assertThat(result.get().status()).isInstanceOf(xyz.charks.outbox.core.Archived.class);
        }

        @Test
        @DisplayName("handles unknown status as PENDING")
        void handlesUnknownStatus() throws SQLException {
            UUID eventId = UUID.randomUUID();
            Instant now = Instant.now();
            setupConnectionMocks();
            when(preparedStatement.executeQuery()).thenReturn(resultSet);
            when(resultSet.next()).thenReturn(true);
            setupResultSetMocks(eventId, "UNKNOWN", now, null, null);

            var result = repository.findById(new OutboxEventId(eventId));

            assertThat(result).isPresent();
            assertThat(result.get().status()).isInstanceOf(xyz.charks.outbox.core.Pending.class);
        }

        @Test
        @DisplayName("handles null processedAt for FAILED status")
        void handlesNullProcessedAtForFailed() throws SQLException {
            UUID eventId = UUID.randomUUID();
            Instant now = Instant.now();
            setupConnectionMocks();
            when(preparedStatement.executeQuery()).thenReturn(resultSet);
            when(resultSet.next()).thenReturn(true);
            setupResultSetMocks(eventId, "FAILED", now, null, null);

            var result = repository.findById(new OutboxEventId(eventId));

            assertThat(result).isPresent();
            assertThat(result.get().status()).isInstanceOf(xyz.charks.outbox.core.Failed.class);
        }

        @Test
        @DisplayName("handles headers deserialization")
        void handlesHeadersDeserialization() throws SQLException {
            UUID eventId = UUID.randomUUID();
            Instant now = Instant.now();
            setupConnectionMocks();
            when(preparedStatement.executeQuery()).thenReturn(resultSet);
            when(resultSet.next()).thenReturn(true);
            when(resultSet.getObject("id", UUID.class)).thenReturn(eventId);
            when(resultSet.getString("aggregate_type")).thenReturn("Order");
            when(resultSet.getString("aggregate_id")).thenReturn("order-123");
            when(resultSet.getString("event_type")).thenReturn("OrderCreated");
            when(resultSet.getString("topic")).thenReturn("orders");
            when(resultSet.getString("partition_key")).thenReturn("order-123");
            when(resultSet.getBytes("payload")).thenReturn("{}".getBytes());
            when(resultSet.getString("headers")).thenReturn("{\"key1\":\"value1\",\"key2\":\"value2\"}");
            when(resultSet.getTimestamp("created_at")).thenReturn(Timestamp.from(now));
            when(resultSet.getString("status")).thenReturn("PENDING");
            when(resultSet.getInt("retry_count")).thenReturn(0);
            when(resultSet.getString("last_error")).thenReturn(null);
            when(resultSet.getTimestamp("processed_at")).thenReturn(null);

            var result = repository.findById(new OutboxEventId(eventId));

            assertThat(result).isPresent();
            assertThat(result.get().headers()).containsEntry("key1", "value1");
            assertThat(result.get().headers()).containsEntry("key2", "value2");
        }
    }

    @Nested
    @DisplayName("find")
    class FindTest {

        @Test
        @DisplayName("throws exception for null query")
        void nullQuery() {
            assertThatThrownBy(() -> repository.find(null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("Query");
        }

        @Test
        @DisplayName("wraps SQLException in OutboxPersistenceException")
        void wrapsSqlException() throws SQLException {
            when(dataSource.getConnection()).thenThrow(new SQLException("Connection failed"));

            assertThatThrownBy(() -> repository.find(OutboxQuery.pending(10)))
                    .isInstanceOf(OutboxPersistenceException.class)
                    .hasMessageContaining("Failed to find")
                    .hasCauseInstanceOf(SQLException.class);
        }

        @Test
        @DisplayName("finds events with pending status filter")
        void findsPendingEvents() throws SQLException {
            setupConnectionMocks();
            when(preparedStatement.executeQuery()).thenReturn(resultSet);
            when(resultSet.next()).thenReturn(false);

            repository.find(OutboxQuery.pending(10));

            verify(preparedStatement).executeQuery();
        }

        @Test
        @DisplayName("finds events with published status filter")
        void findsPublishedEvents() throws SQLException {
            setupConnectionMocks();
            when(preparedStatement.executeQuery()).thenReturn(resultSet);
            when(resultSet.next()).thenReturn(false);

            repository.find(OutboxQuery.builder()
                    .status(OutboxStatusFilter.PUBLISHED)
                    .limit(10)
                    .build());

            verify(preparedStatement).executeQuery();
        }

        @Test
        @DisplayName("finds events with failed status filter")
        void findsFailedEvents() throws SQLException {
            setupConnectionMocks();
            when(preparedStatement.executeQuery()).thenReturn(resultSet);
            when(resultSet.next()).thenReturn(false);

            repository.find(OutboxQuery.builder()
                    .status(OutboxStatusFilter.FAILED)
                    .limit(10)
                    .build());

            verify(preparedStatement).executeQuery();
        }

        @Test
        @DisplayName("finds events with archived status filter")
        void findsArchivedEvents() throws SQLException {
            setupConnectionMocks();
            when(preparedStatement.executeQuery()).thenReturn(resultSet);
            when(resultSet.next()).thenReturn(false);

            repository.find(OutboxQuery.builder()
                    .status(OutboxStatusFilter.ARCHIVED)
                    .limit(10)
                    .build());

            verify(preparedStatement).executeQuery();
        }

        @Test
        @DisplayName("finds events with retryable status filter")
        void findsRetryableEvents() throws SQLException {
            setupConnectionMocks();
            when(preparedStatement.executeQuery()).thenReturn(resultSet);
            when(resultSet.next()).thenReturn(false);

            repository.find(OutboxQuery.builder()
                    .status(OutboxStatusFilter.RETRYABLE)
                    .limit(10)
                    .build());

            verify(preparedStatement).executeQuery();
        }

        @Test
        @DisplayName("finds events with aggregate type filter")
        void findsEventsWithAggregateType() throws SQLException {
            setupConnectionMocks();
            when(preparedStatement.executeQuery()).thenReturn(resultSet);
            when(resultSet.next()).thenReturn(false);

            repository.find(OutboxQuery.builder()
                    .aggregateTypes(Set.of("Order", "Payment"))
                    .limit(10)
                    .build());

            verify(preparedStatement).executeQuery();
        }

        @Test
        @DisplayName("finds events with topic filter")
        void findsEventsWithTopic() throws SQLException {
            setupConnectionMocks();
            when(preparedStatement.executeQuery()).thenReturn(resultSet);
            when(resultSet.next()).thenReturn(false);

            repository.find(OutboxQuery.builder()
                    .topics(Set.of("orders", "payments"))
                    .limit(10)
                    .build());

            verify(preparedStatement).executeQuery();
        }

        @Test
        @DisplayName("finds events with date filters")
        void findsEventsWithDateFilters() throws SQLException {
            setupConnectionMocks();
            when(preparedStatement.executeQuery()).thenReturn(resultSet);
            when(resultSet.next()).thenReturn(false);

            repository.find(OutboxQuery.builder()
                    .createdAfter(Instant.now().minusSeconds(3600))
                    .createdBefore(Instant.now())
                    .limit(10)
                    .build());

            verify(preparedStatement).executeQuery();
        }

        @Test
        @DisplayName("finds events with max retry count")
        void findsEventsWithMaxRetryCount() throws SQLException {
            setupConnectionMocks();
            when(preparedStatement.executeQuery()).thenReturn(resultSet);
            when(resultSet.next()).thenReturn(false);

            repository.find(OutboxQuery.builder()
                    .maxRetryCount(5)
                    .limit(10)
                    .build());

            verify(preparedStatement).executeQuery();
        }
    }

    @Nested
    @DisplayName("update")
    class UpdateTest {

        @Test
        @DisplayName("throws exception for null event")
        void nullEvent() {
            assertThatThrownBy(() -> repository.update(null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("Event");
        }

        @Test
        @DisplayName("wraps SQLException in OutboxPersistenceException")
        void wrapsSqlException() throws SQLException {
            OutboxEvent event = createTestEvent();
            when(dataSource.getConnection()).thenThrow(new SQLException("Connection failed"));

            assertThatThrownBy(() -> repository.update(event))
                    .isInstanceOf(OutboxPersistenceException.class)
                    .hasMessageContaining("Failed to update")
                    .hasCauseInstanceOf(SQLException.class);
        }

        @Test
        @DisplayName("throws exception when event not found")
        void eventNotFound() throws SQLException {
            OutboxEvent event = createTestEvent();
            setupConnectionMocks();
            when(preparedStatement.executeUpdate()).thenReturn(0);

            assertThatThrownBy(() -> repository.update(event))
                    .isInstanceOf(OutboxPersistenceException.class)
                    .hasMessageContaining("not found");
        }

        @Test
        @DisplayName("updates event successfully")
        void updatesEventSuccessfully() throws SQLException {
            OutboxEvent event = createTestEvent();
            setupConnectionMocks();
            when(preparedStatement.executeUpdate()).thenReturn(1);

            OutboxEvent result = repository.update(event);

            assertThat(result).isEqualTo(event);
        }

        @Test
        @DisplayName("updates event with null partition key")
        void updatesEventWithNullPartitionKey() throws SQLException {
            OutboxEvent event = OutboxEvent.builder()
                    .aggregateType("Order")
                    .aggregateId(AggregateId.of("order-123"))
                    .eventType(EventType.of("OrderCreated"))
                    .topic("orders")
                    .partitionKey(null)
                    .payload("{}".getBytes(StandardCharsets.UTF_8))
                    .build();
            setupConnectionMocks();
            when(preparedStatement.executeUpdate()).thenReturn(1);

            OutboxEvent result = repository.update(event);

            assertThat(result).isEqualTo(event);
        }

        @Test
        @DisplayName("updates event with headers")
        void updatesEventWithHeaders() throws SQLException {
            OutboxEvent event = OutboxEvent.builder()
                    .aggregateType("Order")
                    .aggregateId(AggregateId.of("order-123"))
                    .eventType(EventType.of("OrderCreated"))
                    .topic("orders")
                    .payload("{}".getBytes(StandardCharsets.UTF_8))
                    .headers(Map.of("key", "value"))
                    .build();
            setupConnectionMocks();
            when(preparedStatement.executeUpdate()).thenReturn(1);

            OutboxEvent result = repository.update(event);

            assertThat(result).isEqualTo(event);
        }
    }

    @Nested
    @DisplayName("updateStatus")
    class UpdateStatusTest {

        @Test
        @DisplayName("throws exception for null ids")
        void nullIds() {
            assertThatThrownBy(() -> repository.updateStatus(null, Published.now()))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("IDs");
        }

        @Test
        @DisplayName("throws exception for null status")
        void nullStatus() {
            assertThatThrownBy(() -> repository.updateStatus(List.of(), null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("Status");
        }

        @Test
        @DisplayName("returns zero for empty list")
        void emptyList() {
            int result = repository.updateStatus(List.of(), Published.now());

            assertThat(result).isZero();
        }

        @Test
        @DisplayName("wraps SQLException in OutboxPersistenceException")
        void wrapsSqlException() throws SQLException {
            when(dataSource.getConnection()).thenThrow(new SQLException("Connection failed"));

            assertThatThrownBy(() -> repository.updateStatus(List.of(OutboxEventId.generate()), Published.now()))
                    .isInstanceOf(OutboxPersistenceException.class)
                    .hasMessageContaining("Failed to update status")
                    .hasCauseInstanceOf(SQLException.class);
        }

        @Test
        @DisplayName("updates status for multiple events")
        void updatesStatusForMultipleEvents() throws SQLException {
            setupConnectionMocks();
            when(preparedStatement.executeUpdate()).thenReturn(2);

            int result = repository.updateStatus(
                    List.of(OutboxEventId.generate(), OutboxEventId.generate()),
                    Published.now()
            );

            assertThat(result).isEqualTo(2);
        }
    }

    @Nested
    @DisplayName("deleteById")
    class DeleteByIdTest {

        @Test
        @DisplayName("throws exception for null id")
        void nullId() {
            assertThatThrownBy(() -> repository.deleteById(null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("ID");
        }

        @Test
        @DisplayName("wraps SQLException in OutboxPersistenceException")
        void wrapsSqlException() throws SQLException {
            when(dataSource.getConnection()).thenThrow(new SQLException("Connection failed"));

            assertThatThrownBy(() -> repository.deleteById(OutboxEventId.generate()))
                    .isInstanceOf(OutboxPersistenceException.class)
                    .hasMessageContaining("Failed to delete")
                    .hasCauseInstanceOf(SQLException.class);
        }

        @Test
        @DisplayName("returns true when event deleted")
        void returnsTrue() throws SQLException {
            setupConnectionMocks();
            when(preparedStatement.executeUpdate()).thenReturn(1);

            boolean result = repository.deleteById(OutboxEventId.generate());

            assertThat(result).isTrue();
        }

        @Test
        @DisplayName("returns false when event not found")
        void returnsFalse() throws SQLException {
            setupConnectionMocks();
            when(preparedStatement.executeUpdate()).thenReturn(0);

            boolean result = repository.deleteById(OutboxEventId.generate());

            assertThat(result).isFalse();
        }
    }

    @Nested
    @DisplayName("delete")
    class DeleteTest {

        @Test
        @DisplayName("throws exception for null query")
        void nullQuery() {
            assertThatThrownBy(() -> repository.delete(null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("Query");
        }

        @Test
        @DisplayName("wraps SQLException in OutboxPersistenceException")
        void wrapsSqlException() throws SQLException {
            when(dataSource.getConnection()).thenThrow(new SQLException("Connection failed"));

            assertThatThrownBy(() -> repository.delete(OutboxQuery.pending(10)))
                    .isInstanceOf(OutboxPersistenceException.class)
                    .hasMessageContaining("Failed to delete")
                    .hasCauseInstanceOf(SQLException.class);
        }

        @Test
        @DisplayName("deletes events matching query")
        void deletesEventsMatchingQuery() throws SQLException {
            setupConnectionMocks();
            when(preparedStatement.executeUpdate()).thenReturn(5);

            int result = repository.delete(OutboxQuery.pending(100));

            assertThat(result).isEqualTo(5);
        }

        @Test
        @DisplayName("deletes with aggregate type filter")
        void deletesWithAggregateTypeFilter() throws SQLException {
            setupConnectionMocks();
            when(preparedStatement.executeUpdate()).thenReturn(3);

            int result = repository.delete(OutboxQuery.builder()
                    .aggregateTypes(Set.of("Order"))
                    .limit(100)
                    .build());

            assertThat(result).isEqualTo(3);
        }

        @Test
        @DisplayName("deletes with topic filter")
        void deletesWithTopicFilter() throws SQLException {
            setupConnectionMocks();
            when(preparedStatement.executeUpdate()).thenReturn(2);

            int result = repository.delete(OutboxQuery.builder()
                    .topics(Set.of("orders"))
                    .limit(100)
                    .build());

            assertThat(result).isEqualTo(2);
        }

        @Test
        @DisplayName("deletes with date filters")
        void deletesWithDateFilters() throws SQLException {
            setupConnectionMocks();
            when(preparedStatement.executeUpdate()).thenReturn(4);

            int result = repository.delete(OutboxQuery.builder()
                    .createdAfter(Instant.now().minusSeconds(3600))
                    .createdBefore(Instant.now())
                    .limit(100)
                    .build());

            assertThat(result).isEqualTo(4);
        }

        @Test
        @DisplayName("deletes without any filter")
        void deletesWithoutFilter() throws SQLException {
            setupConnectionMocks();
            when(preparedStatement.executeUpdate()).thenReturn(10);

            int result = repository.delete(OutboxQuery.builder().limit(100).build());

            assertThat(result).isEqualTo(10);
        }
    }

    @Nested
    @DisplayName("count")
    class CountTest {

        @Test
        @DisplayName("wraps SQLException in OutboxPersistenceException")
        void wrapsSqlException() throws SQLException {
            when(dataSource.getConnection()).thenThrow(new SQLException("Connection failed"));

            assertThatThrownBy(() -> repository.count(null))
                    .isInstanceOf(OutboxPersistenceException.class)
                    .hasMessageContaining("Failed to count")
                    .hasCauseInstanceOf(SQLException.class);
        }

        @Test
        @DisplayName("counts with status filter")
        void countsWithFilter() throws SQLException {
            setupConnectionMocks();
            when(preparedStatement.executeQuery()).thenReturn(resultSet);
            when(resultSet.next()).thenReturn(true);
            when(resultSet.getLong(1)).thenReturn(5L);

            long result = repository.count(OutboxStatusFilter.PENDING);

            assertThat(result).isEqualTo(5);
        }

        @Test
        @DisplayName("counts without filter")
        void countsWithoutFilter() throws SQLException {
            setupConnectionMocks();
            when(preparedStatement.executeQuery()).thenReturn(resultSet);
            when(resultSet.next()).thenReturn(true);
            when(resultSet.getLong(1)).thenReturn(10L);

            long result = repository.count(null);

            assertThat(result).isEqualTo(10);
        }

        @Test
        @DisplayName("returns zero when no results")
        void returnsZero() throws SQLException {
            setupConnectionMocks();
            when(preparedStatement.executeQuery()).thenReturn(resultSet);
            when(resultSet.next()).thenReturn(false);

            long result = repository.count(null);

            assertThat(result).isZero();
        }

        @Test
        @DisplayName("counts with PUBLISHED filter")
        void countsPublished() throws SQLException {
            setupConnectionMocks();
            when(preparedStatement.executeQuery()).thenReturn(resultSet);
            when(resultSet.next()).thenReturn(true);
            when(resultSet.getLong(1)).thenReturn(3L);

            long result = repository.count(OutboxStatusFilter.PUBLISHED);

            assertThat(result).isEqualTo(3);
        }

        @Test
        @DisplayName("counts with FAILED filter")
        void countsFailed() throws SQLException {
            setupConnectionMocks();
            when(preparedStatement.executeQuery()).thenReturn(resultSet);
            when(resultSet.next()).thenReturn(true);
            when(resultSet.getLong(1)).thenReturn(2L);

            long result = repository.count(OutboxStatusFilter.FAILED);

            assertThat(result).isEqualTo(2);
        }

        @Test
        @DisplayName("counts with ARCHIVED filter")
        void countsArchived() throws SQLException {
            setupConnectionMocks();
            when(preparedStatement.executeQuery()).thenReturn(resultSet);
            when(resultSet.next()).thenReturn(true);
            when(resultSet.getLong(1)).thenReturn(7L);

            long result = repository.count(OutboxStatusFilter.ARCHIVED);

            assertThat(result).isEqualTo(7);
        }

        @Test
        @DisplayName("counts with RETRYABLE filter")
        void countsRetryable() throws SQLException {
            setupConnectionMocks();
            when(preparedStatement.executeQuery()).thenReturn(resultSet);
            when(resultSet.next()).thenReturn(true);
            when(resultSet.getLong(1)).thenReturn(4L);

            long result = repository.count(OutboxStatusFilter.RETRYABLE);

            assertThat(result).isEqualTo(4);
        }
    }

    @Nested
    @DisplayName("config")
    class ConfigTest {

        @Test
        @DisplayName("returns config")
        void returnsConfig() {
            JdbcOutboxConfig config = JdbcOutboxConfig.postgresql();
            JdbcOutboxRepository repo = new JdbcOutboxRepository(dataSource, config);

            assertThat(repo.config()).isSameAs(config);
        }
    }

    private void setupConnectionMocks() throws SQLException {
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
    }

    private void setupResultSetMocks(UUID eventId, String status, Instant createdAt,
                                      Instant processedAt, String lastError) throws SQLException {
        when(resultSet.getObject("id", UUID.class)).thenReturn(eventId);
        when(resultSet.getString("aggregate_type")).thenReturn("Order");
        when(resultSet.getString("aggregate_id")).thenReturn("order-123");
        when(resultSet.getString("event_type")).thenReturn("OrderCreated");
        when(resultSet.getString("topic")).thenReturn("orders");
        when(resultSet.getString("partition_key")).thenReturn("order-123");
        when(resultSet.getBytes("payload")).thenReturn("{}".getBytes());
        when(resultSet.getString("headers")).thenReturn("{}");
        when(resultSet.getTimestamp("created_at")).thenReturn(Timestamp.from(createdAt));
        when(resultSet.getString("status")).thenReturn(status);
        when(resultSet.getInt("retry_count")).thenReturn(lastError != null ? 3 : 0);
        when(resultSet.getString("last_error")).thenReturn(lastError);
        when(resultSet.getTimestamp("processed_at")).thenReturn(
                processedAt != null ? Timestamp.from(processedAt) : null);
    }

    private OutboxEvent createTestEvent() {
        return OutboxEvent.builder()
                .aggregateType("Order")
                .aggregateId(AggregateId.of("order-123"))
                .eventType(EventType.of("OrderCreated"))
                .topic("orders")
                .partitionKey("order-123")
                .payload("{\"test\":true}".getBytes(StandardCharsets.UTF_8))
                .build();
    }
}
