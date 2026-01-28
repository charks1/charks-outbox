package xyz.charks.outbox.r2dbc;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Statement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import xyz.charks.outbox.core.AggregateId;
import xyz.charks.outbox.core.EventType;
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
import java.util.UUID;
import java.util.function.BiFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
@DisplayName("R2dbcOutboxRepository")
class R2dbcOutboxRepositoryTest {

    @Mock
    private ConnectionFactory connectionFactory;

    @Mock
    private Connection connection;

    @Mock
    private Statement statement;

    @Mock
    private Result result;

    @Mock
    private Row row;

    @Mock
    private RowMetadata rowMetadata;

    private R2dbcOutboxRepository repository;

    @BeforeEach
    void setUp() {
        R2dbcOutboxConfig config = R2dbcOutboxConfig.builder()
                .connectionFactory(connectionFactory)
                .tableName("outbox_events")
                .build();
        repository = new R2dbcOutboxRepository(config);
    }

    private void setupConnectionMock() {
        Publisher<Connection> publisher = Mono.just(connection);
        doReturn(publisher).when(connectionFactory).create();
        doReturn(Mono.empty()).when(connection).close();
    }

    private void setupStatementMock() {
        lenient().doReturn(statement).when(connection).createStatement(anyString());
        lenient().doReturn(statement).when(statement).bind(anyString(), any());
        lenient().doReturn(statement).when(statement).bindNull(anyString(), any(Class.class));
    }

    private void setupExecuteWithRowsUpdated(long rowsUpdated) {
        Publisher<Result> executePublisher = Flux.just(result);
        doReturn(executePublisher).when(statement).execute();
        doReturn(Mono.just(rowsUpdated)).when(result).getRowsUpdated();
    }

    @Nested
    @DisplayName("constructor")
    class ConstructorTest {

        @Test
        @DisplayName("throws exception for null config")
        void nullConfig() {
            assertThatThrownBy(() -> new R2dbcOutboxRepository(null))
                    .isInstanceOf(NullPointerException.class);
        }
    }

    @Nested
    @DisplayName("save")
    class SaveTest {

        @Test
        @DisplayName("throws exception for null event")
        void nullEvent() {
            assertThatThrownBy(() -> repository.save(null))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        @DisplayName("saves event and returns it")
        void savesEvent() {
            setupConnectionMock();
            setupStatementMock();
            setupExecuteWithRowsUpdated(1L);

            OutboxEvent event = createTestEvent();
            OutboxEvent result = repository.save(event);

            assertThat(result).isEqualTo(event);
            verify(statement).execute();
        }

        @Test
        @DisplayName("saves event with null partition key")
        void savesEventWithNullPartitionKey() {
            setupConnectionMock();
            setupStatementMock();
            setupExecuteWithRowsUpdated(1L);

            OutboxEvent event = OutboxEvent.builder()
                    .aggregateType("Order")
                    .aggregateId(AggregateId.of("order-123"))
                    .eventType(EventType.of("OrderCreated"))
                    .topic("orders")
                    .partitionKey(null)
                    .payload("{}".getBytes(StandardCharsets.UTF_8))
                    .build();

            OutboxEvent result = repository.save(event);

            assertThat(result.partitionKey()).isNull();
            verify(statement, atLeastOnce()).bindNull(anyString(), any(Class.class));
        }

        @Test
        @DisplayName("saves event with headers")
        void savesEventWithHeaders() {
            setupConnectionMock();
            setupStatementMock();
            setupExecuteWithRowsUpdated(1L);

            OutboxEvent event = OutboxEvent.builder()
                    .aggregateType("Order")
                    .aggregateId(AggregateId.of("order-123"))
                    .eventType(EventType.of("OrderCreated"))
                    .topic("orders")
                    .payload("{}".getBytes(StandardCharsets.UTF_8))
                    .headers(Map.of("key1", "value1"))
                    .build();

            OutboxEvent result = repository.save(event);

            assertThat(result.headers()).containsEntry("key1", "value1");
        }
    }

    @Nested
    @DisplayName("saveAll")
    class SaveAllTest {

        @Test
        @DisplayName("throws exception for null list")
        void nullList() {
            assertThatThrownBy(() -> repository.saveAll(null))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        @DisplayName("returns empty list for empty input")
        void emptyInput() {
            List<OutboxEvent> result = repository.saveAll(List.of());

            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("saves multiple events")
        void savesMultipleEvents() {
            setupConnectionMock();
            setupStatementMock();
            setupExecuteWithRowsUpdated(1L);

            List<OutboxEvent> events = List.of(createTestEvent(), createTestEvent());
            List<OutboxEvent> result = repository.saveAll(events);

            assertThat(result).hasSize(2);
        }
    }

    @Nested
    @DisplayName("findById")
    class FindByIdTest {

        @Test
        @DisplayName("throws exception for null id")
        void nullId() {
            assertThatThrownBy(() -> repository.findById(null))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        @DisplayName("returns empty when not found")
        void returnsEmptyWhenNotFound() {
            setupConnectionMock();
            setupStatementMock();

            Publisher<Result> executePublisher = Flux.just(result);
            doReturn(executePublisher).when(statement).execute();
            doReturn(Flux.empty()).when(result).map(any(BiFunction.class));

            var found = repository.findById(OutboxEventId.generate());

            assertThat(found).isEmpty();
        }

        @Test
        @DisplayName("returns event when found")
        void returnsEventWhenFound() {
            setupConnectionMock();
            setupStatementMock();

            UUID eventId = UUID.randomUUID();
            Instant now = Instant.now();

            Publisher<Result> executePublisher = Flux.just(result);
            doReturn(executePublisher).when(statement).execute();

            OutboxEvent expectedEvent = OutboxEvent.builder()
                    .id(new OutboxEventId(eventId))
                    .aggregateType("Order")
                    .aggregateId(AggregateId.of("order-123"))
                    .eventType(EventType.of("OrderCreated"))
                    .topic("orders")
                    .payload("{}".getBytes())
                    .status(Pending.at(now))
                    .createdAt(now)
                    .build();

            doReturn(Flux.just(expectedEvent)).when(result).map(any(BiFunction.class));

            var found = repository.findById(new OutboxEventId(eventId));

            assertThat(found).isPresent();
            assertThat(found.get().id().value()).isEqualTo(eventId);
        }
    }

    @Nested
    @DisplayName("find")
    class FindTest {

        @Test
        @DisplayName("throws exception for null query")
        void nullQuery() {
            assertThatThrownBy(() -> repository.find(null))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        @DisplayName("finds events with pending status filter")
        void findsPendingEvents() {
            setupConnectionMock();
            setupStatementMock();

            OutboxEvent event = createTestEvent();

            Publisher<Result> executePublisher = Flux.just(result);
            doReturn(executePublisher).when(statement).execute();
            doReturn(Flux.just(event)).when(result).map(any(BiFunction.class));

            List<OutboxEvent> found = repository.find(OutboxQuery.pending(10));

            assertThat(found).hasSize(1);
        }

        @Test
        @DisplayName("finds events with aggregate type filter")
        void findsEventsWithAggregateType() {
            setupConnectionMock();
            setupStatementMock();

            OutboxEvent event = createTestEvent();

            Publisher<Result> executePublisher = Flux.just(result);
            doReturn(executePublisher).when(statement).execute();
            doReturn(Flux.just(event)).when(result).map(any(BiFunction.class));

            List<OutboxEvent> found = repository.find(OutboxQuery.builder()
                    .aggregateTypes(java.util.Set.of("Order"))
                    .limit(10)
                    .build());

            assertThat(found).hasSize(1);
        }

        @Test
        @DisplayName("returns empty list when no events found")
        void returnsEmptyList() {
            setupConnectionMock();
            setupStatementMock();

            Publisher<Result> executePublisher = Flux.just(result);
            doReturn(executePublisher).when(statement).execute();
            doReturn(Flux.empty()).when(result).map(any(BiFunction.class));

            List<OutboxEvent> found = repository.find(OutboxQuery.pending(10));

            assertThat(found).isEmpty();
        }
    }

    @Nested
    @DisplayName("update")
    class UpdateTest {

        @Test
        @DisplayName("throws exception for null event")
        void nullEvent() {
            assertThatThrownBy(() -> repository.update(null))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        @DisplayName("throws exception when event not found")
        void eventNotFound() {
            setupConnectionMock();
            setupStatementMock();
            setupExecuteWithRowsUpdated(0L);

            OutboxEvent event = createTestEvent();

            assertThatThrownBy(() -> repository.update(event))
                    .isInstanceOf(OutboxPersistenceException.class)
                    .hasMessageContaining("not found");
        }

        @Test
        @DisplayName("updates event successfully")
        void updatesEventSuccessfully() {
            setupConnectionMock();
            setupStatementMock();
            setupExecuteWithRowsUpdated(1L);

            OutboxEvent event = createTestEvent();
            OutboxEvent result = repository.update(event);

            assertThat(result).isEqualTo(event);
            verify(statement).execute();
        }
    }

    @Nested
    @DisplayName("updateStatus")
    class UpdateStatusTest {

        @Test
        @DisplayName("throws exception for null ids")
        void nullIds() {
            assertThatThrownBy(() -> repository.updateStatus(null, Published.now()))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        @DisplayName("throws exception for null status")
        void nullStatus() {
            assertThatThrownBy(() -> repository.updateStatus(List.of(), null))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        @DisplayName("returns zero for empty list")
        void emptyList() {
            int result = repository.updateStatus(List.of(), Published.now());

            assertThat(result).isZero();
        }

        @Test
        @DisplayName("updates status for multiple events")
        void updatesStatusForMultipleEvents() {
            setupConnectionMock();
            setupStatementMock();
            setupExecuteWithRowsUpdated(1L);

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
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        @DisplayName("returns true when event deleted")
        void returnsTrue() {
            setupConnectionMock();
            setupStatementMock();
            setupExecuteWithRowsUpdated(1L);

            boolean result = repository.deleteById(OutboxEventId.generate());

            assertThat(result).isTrue();
        }

        @Test
        @DisplayName("returns false when event not found")
        void returnsFalse() {
            setupConnectionMock();
            setupStatementMock();
            setupExecuteWithRowsUpdated(0L);

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
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        @DisplayName("deletes events matching query")
        void deletesEventsMatchingQuery() {
            setupConnectionMock();
            setupStatementMock();
            setupExecuteWithRowsUpdated(3L);

            int result = repository.delete(OutboxQuery.pending(100));

            assertThat(result).isEqualTo(3);
        }

        @Test
        @DisplayName("deletes without filter")
        void deletesWithoutFilter() {
            setupConnectionMock();
            setupStatementMock();
            setupExecuteWithRowsUpdated(5L);

            int result = repository.delete(OutboxQuery.builder().limit(100).build());

            assertThat(result).isEqualTo(5);
        }
    }

    @Nested
    @DisplayName("count")
    class CountTest {

        @Test
        @DisplayName("returns count value")
        void returnsCountValue() {
            setupConnectionMock();
            setupStatementMock();

            Publisher<Result> executePublisher = Flux.just(result);
            doReturn(executePublisher).when(statement).execute();
            doReturn(Flux.just(5L)).when(result).map(any(BiFunction.class));

            long count = repository.count(null);

            assertThat(count).isEqualTo(5);
        }

        @Test
        @DisplayName("counts with pending status filter")
        void countsPendingEvents() {
            setupConnectionMock();
            setupStatementMock();

            Publisher<Result> executePublisher = Flux.just(result);
            doReturn(executePublisher).when(statement).execute();
            doReturn(Flux.just(3L)).when(result).map(any(BiFunction.class));

            long count = repository.count(OutboxStatusFilter.PENDING);

            assertThat(count).isEqualTo(3);
        }

        @Test
        @DisplayName("returns zero when no results")
        void returnsZeroWhenNoResults() {
            setupConnectionMock();
            setupStatementMock();

            Publisher<Result> executePublisher = Flux.just(result);
            doReturn(executePublisher).when(statement).execute();
            doReturn(Flux.empty()).when(result).map(any(BiFunction.class));

            long count = repository.count(null);

            assertThat(count).isZero();
        }

        @Test
        @DisplayName("counts with published status filter")
        void countsPublishedEvents() {
            setupConnectionMock();
            setupStatementMock();

            Publisher<Result> executePublisher = Flux.just(result);
            doReturn(executePublisher).when(statement).execute();
            doReturn(Flux.just(7L)).when(result).map(any(BiFunction.class));

            long count = repository.count(OutboxStatusFilter.PUBLISHED);

            assertThat(count).isEqualTo(7);
        }

        @Test
        @DisplayName("counts with failed status filter")
        void countsFailedEvents() {
            setupConnectionMock();
            setupStatementMock();

            Publisher<Result> executePublisher = Flux.just(result);
            doReturn(executePublisher).when(statement).execute();
            doReturn(Flux.just(2L)).when(result).map(any(BiFunction.class));

            long count = repository.count(OutboxStatusFilter.FAILED);

            assertThat(count).isEqualTo(2);
        }

        @Test
        @DisplayName("counts with archived status filter")
        void countsArchivedEvents() {
            setupConnectionMock();
            setupStatementMock();

            Publisher<Result> executePublisher = Flux.just(result);
            doReturn(executePublisher).when(statement).execute();
            doReturn(Flux.just(4L)).when(result).map(any(BiFunction.class));

            long count = repository.count(OutboxStatusFilter.ARCHIVED);

            assertThat(count).isEqualTo(4);
        }

        @Test
        @DisplayName("counts with retryable status filter")
        void countsRetryableEvents() {
            setupConnectionMock();
            setupStatementMock();

            Publisher<Result> executePublisher = Flux.just(result);
            doReturn(executePublisher).when(statement).execute();
            doReturn(Flux.just(6L)).when(result).map(any(BiFunction.class));

            long count = repository.count(OutboxStatusFilter.RETRYABLE);

            assertThat(count).isEqualTo(6);
        }
    }

    @Nested
    @DisplayName("R2dbcOutboxConfig")
    class ConfigTest {

        @Test
        @DisplayName("uses default table name when not specified")
        void usesDefaultTableName() {
            R2dbcOutboxConfig config = R2dbcOutboxConfig.builder()
                    .connectionFactory(connectionFactory)
                    .build();

            assertThat(config.tableName()).isEqualTo("outbox_events");
        }

        @Test
        @DisplayName("uses custom table name when specified")
        void usesCustomTableName() {
            R2dbcOutboxConfig config = R2dbcOutboxConfig.builder()
                    .connectionFactory(connectionFactory)
                    .tableName("custom_outbox")
                    .build();

            assertThat(config.tableName()).isEqualTo("custom_outbox");
        }

        @Test
        @DisplayName("throws exception for null connectionFactory")
        void throwsExceptionForNullConnectionFactory() {
            assertThatThrownBy(() -> R2dbcOutboxConfig.builder().build())
                    .isInstanceOf(NullPointerException.class);
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
