package xyz.charks.outbox.r2dbc;

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.ConnectionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Mono;
import xyz.charks.outbox.core.AggregateId;
import xyz.charks.outbox.core.Archived;
import xyz.charks.outbox.core.EventType;
import xyz.charks.outbox.core.Failed;
import xyz.charks.outbox.core.OutboxEvent;
import xyz.charks.outbox.core.OutboxEventId;
import xyz.charks.outbox.core.OutboxQuery;
import xyz.charks.outbox.core.OutboxStatusFilter;
import xyz.charks.outbox.core.Pending;
import xyz.charks.outbox.core.Published;
import xyz.charks.outbox.exception.OutboxPersistenceException;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for R2dbcOutboxRepository using Testcontainers with PostgreSQL.
 */
@Testcontainers
@DisplayName("R2dbcOutboxRepository Integration Tests")
class R2dbcOutboxRepositoryIT {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("outbox_test")
            .withUsername("test")
            .withPassword("test");

    private static ConnectionFactory connectionFactory;
    private R2dbcOutboxRepository repository;

    @BeforeAll
    static void setupConnectionFactory() {
        PostgresqlConnectionConfiguration config = PostgresqlConnectionConfiguration.builder()
                .host(postgres.getHost())
                .port(postgres.getMappedPort(5432))
                .database(postgres.getDatabaseName())
                .username(postgres.getUsername())
                .password(postgres.getPassword())
                .build();

        connectionFactory = new PostgresqlConnectionFactory(config);
    }

    @BeforeEach
    void setUp() {
        createTable();
        R2dbcOutboxConfig config = R2dbcOutboxConfig.builder()
                .connectionFactory(connectionFactory)
                .tableName("outbox_events")
                .build();
        repository = new R2dbcOutboxRepository(config);
    }

    @AfterEach
    void tearDown() {
        truncateTable();
    }

    private void createTable() {
        String sql = """
                CREATE TABLE IF NOT EXISTS outbox_events (
                    id UUID PRIMARY KEY,
                    aggregate_type VARCHAR(255) NOT NULL,
                    aggregate_id VARCHAR(255) NOT NULL,
                    event_type VARCHAR(255) NOT NULL,
                    topic VARCHAR(255) NOT NULL,
                    partition_key VARCHAR(255),
                    payload BYTEA NOT NULL,
                    headers TEXT,
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    status VARCHAR(50) NOT NULL,
                    retry_count INTEGER DEFAULT 0,
                    last_error TEXT,
                    processed_at TIMESTAMP WITH TIME ZONE
                )
                """;
        executeStatement(sql);
    }

    private void truncateTable() {
        executeStatement("TRUNCATE TABLE outbox_events");
    }

    private void executeStatement(String sql) {
        Mono.from(connectionFactory.create())
                .flatMap(conn -> Mono.from(conn.createStatement(sql).execute())
                        .then(Mono.from(conn.close())))
                .block();
    }

    @Nested
    @DisplayName("save")
    class SaveIT {

        @Test
        @DisplayName("persists event to database and retrieves it")
        void savesAndRetrievesEvent() {
            OutboxEvent event = createTestEvent();

            OutboxEvent saved = repository.save(event);
            Optional<OutboxEvent> found = repository.findById(event.id());

            assertThat(saved).isEqualTo(event);
            assertThat(found).isPresent();
            assertThat(found.get().id()).isEqualTo(event.id());
            assertThat(found.get().aggregateType()).isEqualTo(event.aggregateType());
            assertThat(found.get().aggregateId()).isEqualTo(event.aggregateId());
            assertThat(found.get().eventType()).isEqualTo(event.eventType());
            assertThat(found.get().topic()).isEqualTo(event.topic());
            assertThat(found.get().partitionKey()).isEqualTo(event.partitionKey());
            assertThat(found.get().payload()).isEqualTo(event.payload());
        }

        @Test
        @DisplayName("persists event with null partition key")
        void savesEventWithNullPartitionKey() {
            OutboxEvent event = OutboxEvent.builder()
                    .aggregateType("Order")
                    .aggregateId(AggregateId.of("order-123"))
                    .eventType(EventType.of("OrderCreated"))
                    .topic("orders")
                    .partitionKey(null)
                    .payload("{}".getBytes(StandardCharsets.UTF_8))
                    .build();

            repository.save(event);
            Optional<OutboxEvent> found = repository.findById(event.id());

            assertThat(found).isPresent();
            assertThat(found.get().partitionKey()).isNull();
        }

        @Test
        @DisplayName("persists and retrieves headers correctly")
        void savesAndRetrievesHeaders() {
            OutboxEvent event = OutboxEvent.builder()
                    .aggregateType("Order")
                    .aggregateId(AggregateId.of("order-123"))
                    .eventType(EventType.of("OrderCreated"))
                    .topic("orders")
                    .payload("{}".getBytes(StandardCharsets.UTF_8))
                    .headers(Map.of("traceId", "abc123", "correlationId", "xyz789"))
                    .build();

            repository.save(event);
            Optional<OutboxEvent> found = repository.findById(event.id());

            assertThat(found).isPresent();
            assertThat(found.get().headers())
                    .containsEntry("traceId", "abc123")
                    .containsEntry("correlationId", "xyz789");
        }
    }

    @Nested
    @DisplayName("saveAll")
    class SaveAllIT {

        @Test
        @DisplayName("persists multiple events in batch")
        void savesMultipleEvents() {
            List<OutboxEvent> events = List.of(
                    createTestEvent(),
                    createTestEvent(),
                    createTestEvent()
            );

            List<OutboxEvent> saved = repository.saveAll(events);

            assertThat(saved).hasSize(3);
            assertThat(repository.count(null)).isEqualTo(3);
        }
    }

    @Nested
    @DisplayName("findById")
    class FindByIdIT {

        @Test
        @DisplayName("returns empty when event does not exist")
        void returnsEmptyForNonExistent() {
            Optional<OutboxEvent> result = repository.findById(OutboxEventId.generate());

            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("returns event with correct status mapping")
        void returnsEventWithCorrectStatus() {
            OutboxEvent event = createTestEvent();
            repository.save(event);

            OutboxEvent found = repository.findById(event.id()).orElseThrow();

            assertThat(found.status()).isInstanceOf(Pending.class);
        }
    }

    @Nested
    @DisplayName("find")
    class FindIT {

        @Test
        @DisplayName("finds pending events ordered by creation time")
        void findsPendingEvents() {
            OutboxEvent event1 = createTestEvent();
            OutboxEvent event2 = createTestEvent();
            repository.save(event1);
            repository.save(event2);

            List<OutboxEvent> result = repository.find(OutboxQuery.pending(10));

            assertThat(result).hasSize(2);
        }

        @Test
        @DisplayName("respects limit parameter")
        void respectsLimit() {
            for (int i = 0; i < 5; i++) {
                repository.save(createTestEvent());
            }

            List<OutboxEvent> result = repository.find(OutboxQuery.pending(3));

            assertThat(result).hasSize(3);
        }

        @Test
        @DisplayName("filters by aggregate type")
        void filtersByAggregateType() {
            repository.save(OutboxEvent.builder()
                    .aggregateType("Order")
                    .aggregateId(AggregateId.of("o1"))
                    .eventType(EventType.of("Created"))
                    .topic("orders")
                    .payload("{}".getBytes())
                    .build());
            repository.save(OutboxEvent.builder()
                    .aggregateType("Payment")
                    .aggregateId(AggregateId.of("p1"))
                    .eventType(EventType.of("Created"))
                    .topic("payments")
                    .payload("{}".getBytes())
                    .build());

            List<OutboxEvent> result = repository.find(OutboxQuery.builder()
                    .aggregateTypes(java.util.Set.of("Order"))
                    .limit(10)
                    .build());

            assertThat(result).hasSize(1);
            assertThat(result.getFirst().aggregateType()).isEqualTo("Order");
        }
    }

    @Nested
    @DisplayName("update")
    class UpdateIT {

        @Test
        @DisplayName("updates existing event")
        void updatesExistingEvent() {
            OutboxEvent event = createTestEvent();
            repository.save(event);

            OutboxEvent updated = OutboxEvent.builder()
                    .id(event.id())
                    .aggregateType("UpdatedType")
                    .aggregateId(event.aggregateId())
                    .eventType(event.eventType())
                    .topic("updated-topic")
                    .payload("updated".getBytes())
                    .status(event.status())
                    .createdAt(event.createdAt())
                    .build();

            repository.update(updated);

            OutboxEvent found = repository.findById(event.id()).orElseThrow();
            assertThat(found.aggregateType()).isEqualTo("UpdatedType");
            assertThat(found.topic()).isEqualTo("updated-topic");
        }

        @Test
        @DisplayName("throws exception for non-existent event")
        void throwsForNonExistent() {
            OutboxEvent event = createTestEvent();

            assertThatThrownBy(() -> repository.update(event))
                    .isInstanceOf(OutboxPersistenceException.class);
        }
    }

    @Nested
    @DisplayName("updateStatus")
    class UpdateStatusIT {

        @Test
        @DisplayName("updates status to PUBLISHED")
        void updatesToPublished() {
            OutboxEvent event = createTestEvent();
            repository.save(event);

            int updated = repository.updateStatus(List.of(event.id()), Published.now());

            assertThat(updated).isEqualTo(1);
            OutboxEvent found = repository.findById(event.id()).orElseThrow();
            assertThat(found.status()).isInstanceOf(Published.class);
        }

        @Test
        @DisplayName("updates status to FAILED with error message")
        void updatesToFailed() {
            OutboxEvent event = createTestEvent();
            repository.save(event);

            repository.updateStatus(List.of(event.id()),
                    new Failed("Connection timeout", 1, Instant.now()));

            OutboxEvent found = repository.findById(event.id()).orElseThrow();
            assertThat(found.status()).isInstanceOf(Failed.class);
            Failed failed = (Failed) found.status();
            assertThat(failed.lastError()).isEqualTo("Connection timeout");
        }

        @Test
        @DisplayName("updates status to ARCHIVED")
        void updatesToArchived() {
            OutboxEvent event = createTestEvent();
            repository.save(event);

            repository.updateStatus(List.of(event.id()),
                    new Archived(Instant.now(), "Manually archived"));

            OutboxEvent found = repository.findById(event.id()).orElseThrow();
            assertThat(found.status()).isInstanceOf(Archived.class);
        }

        @Test
        @DisplayName("updates multiple events at once")
        void updatesMultipleEvents() {
            OutboxEvent event1 = createTestEvent();
            OutboxEvent event2 = createTestEvent();
            repository.save(event1);
            repository.save(event2);

            int updated = repository.updateStatus(
                    List.of(event1.id(), event2.id()),
                    Published.now()
            );

            assertThat(updated).isEqualTo(2);
        }
    }

    @Nested
    @DisplayName("deleteById")
    class DeleteByIdIT {

        @Test
        @DisplayName("deletes existing event")
        void deletesExistingEvent() {
            OutboxEvent event = createTestEvent();
            repository.save(event);

            boolean deleted = repository.deleteById(event.id());

            assertThat(deleted).isTrue();
            assertThat(repository.findById(event.id())).isEmpty();
        }

        @Test
        @DisplayName("returns false for non-existent event")
        void returnsFalseForNonExistent() {
            boolean deleted = repository.deleteById(OutboxEventId.generate());

            assertThat(deleted).isFalse();
        }
    }

    @Nested
    @DisplayName("delete")
    class DeleteIT {

        @Test
        @DisplayName("deletes events matching query")
        void deletesMatchingEvents() {
            for (int i = 0; i < 3; i++) {
                repository.save(createTestEvent());
            }

            int deleted = repository.delete(OutboxQuery.pending(100));

            assertThat(deleted).isEqualTo(3);
            assertThat(repository.count(null)).isZero();
        }
    }

    @Nested
    @DisplayName("count")
    class CountIT {

        @Test
        @DisplayName("counts all events")
        void countsAllEvents() {
            for (int i = 0; i < 5; i++) {
                repository.save(createTestEvent());
            }

            long count = repository.count(null);

            assertThat(count).isEqualTo(5);
        }

        @Test
        @DisplayName("counts by status filter")
        void countsByStatusFilter() {
            OutboxEvent event1 = createTestEvent();
            OutboxEvent event2 = createTestEvent();
            repository.save(event1);
            repository.save(event2);
            repository.updateStatus(List.of(event1.id()), Published.now());

            assertThat(repository.count(OutboxStatusFilter.PENDING)).isEqualTo(1);
            assertThat(repository.count(OutboxStatusFilter.PUBLISHED)).isEqualTo(1);
            assertThat(repository.count(OutboxStatusFilter.FAILED)).isZero();
        }
    }

    private OutboxEvent createTestEvent() {
        return OutboxEvent.builder()
                .aggregateType("Order")
                .aggregateId(AggregateId.of("order-" + UUID.randomUUID()))
                .eventType(EventType.of("OrderCreated"))
                .topic("orders")
                .partitionKey("partition-1")
                .payload("{\"test\":true}".getBytes(StandardCharsets.UTF_8))
                .build();
    }
}
