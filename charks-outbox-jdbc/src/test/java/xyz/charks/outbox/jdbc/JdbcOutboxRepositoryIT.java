package xyz.charks.outbox.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import xyz.charks.outbox.core.*;
import xyz.charks.outbox.exception.OutboxPersistenceException;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
@DisplayName("JdbcOutboxRepository Integration Tests")
class JdbcOutboxRepositoryIT {

    @Container
    @SuppressWarnings("resource") // Lifecycle managed by Testcontainers
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("testdb")
            .withUsername("test")
            .withPassword("test");

    private static HikariDataSource dataSource;
    private JdbcOutboxRepository repository;

    @BeforeAll
    static void setupDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(postgres.getJdbcUrl());
        config.setUsername(postgres.getUsername());
        config.setPassword(postgres.getPassword());
        config.setMaximumPoolSize(5);
        dataSource = new HikariDataSource(config);
    }

    @BeforeEach
    void setup() throws Exception {
        JdbcOutboxConfig config = JdbcOutboxConfig.postgresql();
        repository = new JdbcOutboxRepository(dataSource, config);

        OutboxTableSchema schema = OutboxTableSchema.forDialect(SqlDialect.POSTGRESQL);

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(schema.dropTableStatement("outbox_events", true));
            stmt.execute(schema.createTableStatement("outbox_events"));
            stmt.execute(schema.createPendingIndexStatement("outbox_events"));
        }
    }

    @AfterEach
    @SuppressWarnings("SqlNoDataSourceInspection") // Datasource configured at runtime via Testcontainers
    void cleanup() throws Exception {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("TRUNCATE TABLE outbox_events");
        }
    }

    @Nested
    @DisplayName("save")
    class SaveTest {

        @Test
        @DisplayName("persists event and returns it")
        void savesEvent() {
            OutboxEvent event = createTestEvent();

            OutboxEvent saved = repository.save(event);

            assertThat(saved).isEqualTo(event);
            assertThat(repository.findById(event.id())).isPresent();
        }

        @Test
        @DisplayName("persists event with headers")
        void savesEventWithHeaders() {
            OutboxEvent event = OutboxEvent.builder()
                    .aggregateType("Order")
                    .aggregateId(AggregateId.of("order-123"))
                    .eventType(EventType.of("OrderCreated"))
                    .topic("orders")
                    .payload("{\"orderId\":\"123\"}".getBytes(StandardCharsets.UTF_8))
                    .headers(Map.of(
                            "correlation-id", "abc-123",
                            "content-type", "application/json"
                    ))
                    .build();

            repository.save(event);
            Optional<OutboxEvent> found = repository.findById(event.id());

            assertThat(found).isPresent();
            assertThat(found.get().headers())
                    .containsEntry("correlation-id", "abc-123")
                    .containsEntry("content-type", "application/json");
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
                    .payload("test".getBytes(StandardCharsets.UTF_8))
                    .build();

            repository.save(event);
            Optional<OutboxEvent> found = repository.findById(event.id());

            assertThat(found).isPresent();
            assertThat(found.get().partitionKey()).isNull();
        }

        @Test
        @DisplayName("throws exception for null event")
        @SuppressWarnings("DataFlowIssue")
        void nullEvent() {
            assertThatThrownBy(() -> repository.save(null))
                    .isInstanceOf(NullPointerException.class);
        }
    }

    @Nested
    @DisplayName("saveAll")
    class SaveAllTest {

        @Test
        @DisplayName("persists multiple events in batch")
        void savesMultipleEvents() {
            List<OutboxEvent> events = List.of(
                    createTestEvent("order-1"),
                    createTestEvent("order-2"),
                    createTestEvent("order-3")
            );

            repository.saveAll(events);

            assertThat(repository.count(null)).isEqualTo(3);
        }

        @Test
        @DisplayName("returns empty list for empty input")
        void emptyInput() {
            List<OutboxEvent> result = repository.saveAll(List.of());
            assertThat(result).isEmpty();
        }
    }

    @Nested
    @DisplayName("findById")
    class FindByIdTest {

        @Test
        @DisplayName("finds existing event")
        void findsExistingEvent() {
            OutboxEvent event = createTestEvent();
            repository.save(event);

            Optional<OutboxEvent> found = repository.findById(event.id());

            assertThat(found).isPresent();
            assertThat(found.get().id()).isEqualTo(event.id());
            assertThat(found.get().aggregateType()).isEqualTo("Order");
            assertThat(found.get().topic()).isEqualTo("orders");
        }

        @Test
        @DisplayName("returns empty for non-existent event")
        void notFound() {
            Optional<OutboxEvent> found = repository.findById(OutboxEventId.generate());
            assertThat(found).isEmpty();
        }
    }

    @Nested
    @DisplayName("find")
    class FindTest {

        @BeforeEach
        void setupTestData() {
            repository.save(createTestEvent("order-1", "Order"));
            repository.save(createTestEvent("order-2", "Order"));
            repository.save(createTestEvent("payment-1", "Payment"));
        }

        @Test
        @DisplayName("finds pending events")
        void findsPendingEvents() {
            OutboxQuery query = OutboxQuery.pending(10);

            List<OutboxEvent> events = repository.find(query);

            assertThat(events).hasSize(3).allMatch(e -> e.status() instanceof Pending);
        }

        @Test
        @DisplayName("limits result size")
        void limitsResults() {
            OutboxQuery query = OutboxQuery.builder()
                    .status(OutboxStatusFilter.PENDING)
                    .limit(2)
                    .build();

            List<OutboxEvent> events = repository.find(query);

            assertThat(events).hasSize(2);
        }

        @Test
        @DisplayName("filters by aggregate type")
        void filtersByAggregateType() {
            OutboxQuery query = OutboxQuery.builder()
                    .aggregateType("Order")
                    .limit(10)
                    .build();

            List<OutboxEvent> events = repository.find(query);

            assertThat(events).hasSize(2).allMatch(e -> e.aggregateType().equals("Order"));
        }

        @Test
        @DisplayName("filters by multiple aggregate types")
        void filtersByMultipleAggregateTypes() {
            OutboxQuery query = OutboxQuery.builder()
                    .aggregateTypes(java.util.Set.of("Order", "Payment"))
                    .limit(10)
                    .build();

            List<OutboxEvent> events = repository.find(query);

            assertThat(events).hasSize(3);
        }

        @Test
        @DisplayName("uses FOR UPDATE SKIP LOCKED")
        void usesSkipLocked() {
            OutboxQuery query = OutboxQuery.builder()
                    .status(OutboxStatusFilter.PENDING)
                    .limit(10)
                    .lockMode(LockMode.FOR_UPDATE_SKIP_LOCKED)
                    .build();

            List<OutboxEvent> events = repository.find(query);

            assertThat(events).hasSize(3);
        }

        @Test
        @DisplayName("orders by created_at ascending")
        void ordersAscending() throws Exception {
            cleanup();
            Instant earlier = Instant.now().minusSeconds(10);
            Instant later = Instant.now();

            OutboxEvent first = createTestEvent("first").toBuilder().createdAt(earlier).build();
            OutboxEvent second = createTestEvent("second").toBuilder().createdAt(later).build();

            repository.save(first);
            repository.save(second);

            OutboxQuery query = OutboxQuery.pending(10);
            List<OutboxEvent> events = repository.find(query);

            assertThat(events).hasSize(2);
            assertThat(events.get(0).aggregateId().value()).isEqualTo("first");
            assertThat(events.get(1).aggregateId().value()).isEqualTo("second");
        }
    }

    @Nested
    @DisplayName("update")
    class UpdateTest {

        @Test
        @DisplayName("updates event status to Published")
        void updatesStatus() {
            OutboxEvent event = createTestEvent();
            repository.save(event);

            OutboxEvent published = event.markPublished(Instant.now());
            repository.update(published);

            Optional<OutboxEvent> found = repository.findById(event.id());
            assertThat(found).isPresent();
            assertThat(found.get().status()).isInstanceOf(Published.class);
        }

        @Test
        @DisplayName("throws exception for non-existent event")
        void nonExistentEvent() {
            OutboxEvent event = createTestEvent();

            assertThatThrownBy(() -> repository.update(event))
                    .isInstanceOf(OutboxPersistenceException.class)
                    .hasMessageContaining("not found");
        }
    }

    @Nested
    @DisplayName("updateStatus")
    class UpdateStatusTest {

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
            assertThat(failed.error()).isEqualTo("Connection timeout");
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
        @DisplayName("updates status for multiple events")
        void updatesMultipleEvents() {
            OutboxEvent e1 = createTestEvent("order-1");
            OutboxEvent e2 = createTestEvent("order-2");
            repository.save(e1);
            repository.save(e2);

            int updated = repository.updateStatus(
                    List.of(e1.id(), e2.id()),
                    Published.now()
            );

            assertThat(updated).isEqualTo(2);
            assertThat(repository.count(OutboxStatusFilter.PUBLISHED)).isEqualTo(2);
        }

        @Test
        @DisplayName("returns zero for empty list")
        void emptyList() {
            int updated = repository.updateStatus(List.of(), Published.now());
            assertThat(updated).isZero();
        }
    }

    @Nested
    @DisplayName("deleteById")
    class DeleteByIdTest {

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
        void nonExistentEvent() {
            boolean deleted = repository.deleteById(OutboxEventId.generate());
            assertThat(deleted).isFalse();
        }
    }

    @Nested
    @DisplayName("delete")
    class DeleteTest {

        @Test
        @DisplayName("deletes events matching query")
        void deletesMatchingEvents() {
            repository.save(createTestEvent("order-1"));
            OutboxEvent e2 = createTestEvent("order-2");
            repository.save(e2);
            repository.update(e2.markPublished(Instant.now()));

            OutboxQuery query = OutboxQuery.builder()
                    .status(OutboxStatusFilter.PUBLISHED)
                    .limit(100)
                    .build();

            int deleted = repository.delete(query);

            assertThat(deleted).isEqualTo(1);
            assertThat(repository.count(OutboxStatusFilter.PENDING)).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("count")
    class CountTest {

        @BeforeEach
        void setupTestData() {
            repository.save(createTestEvent("order-1"));
            repository.save(createTestEvent("order-2"));
            OutboxEvent e3 = createTestEvent("order-3");
            repository.save(e3);
            repository.update(e3.markPublished(Instant.now()));
        }

        @Test
        @DisplayName("counts all events")
        void countsAll() {
            assertThat(repository.count(null)).isEqualTo(3);
        }

        @Test
        @DisplayName("counts pending events")
        void countsPending() {
            assertThat(repository.count(OutboxStatusFilter.PENDING)).isEqualTo(2);
        }

        @Test
        @DisplayName("counts published events")
        void countsPublished() {
            assertThat(repository.count(OutboxStatusFilter.PUBLISHED)).isEqualTo(1);
        }
    }

    private OutboxEvent createTestEvent() {
        return createTestEvent("order-" + System.nanoTime());
    }

    private OutboxEvent createTestEvent(String aggregateId) {
        return createTestEvent(aggregateId, "Order");
    }

    private OutboxEvent createTestEvent(String aggregateId, String aggregateType) {
        return OutboxEvent.builder()
                .aggregateType(aggregateType)
                .aggregateId(AggregateId.of(aggregateId))
                .eventType(EventType.of("OrderCreated"))
                .topic("orders")
                .partitionKey(aggregateId)
                .payload("{\"test\":true}".getBytes(StandardCharsets.UTF_8))
                .build();
    }
}
