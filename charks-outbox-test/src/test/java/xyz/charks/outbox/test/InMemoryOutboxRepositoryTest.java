package xyz.charks.outbox.test;

import xyz.charks.outbox.core.AggregateId;
import xyz.charks.outbox.core.EventType;
import xyz.charks.outbox.core.OutboxEvent;
import xyz.charks.outbox.core.OutboxEventId;
import xyz.charks.outbox.core.OutboxQuery;
import xyz.charks.outbox.core.OutboxStatusFilter;
import xyz.charks.outbox.core.Published;
import xyz.charks.outbox.exception.OutboxPersistenceException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("InMemoryOutboxRepository")
class InMemoryOutboxRepositoryTest {

    private InMemoryOutboxRepository repository;

    @BeforeEach
    void setup() {
        repository = new InMemoryOutboxRepository();
    }

    @Nested
    @DisplayName("save")
    class SaveTest {

        @Test
        @DisplayName("persists event")
        void persistsEvent() {
            OutboxEvent event = createTestEvent();

            repository.save(event);

            assertThat(repository.findById(event.id())).isPresent();
        }

        @Test
        @DisplayName("returns saved event")
        void returnsSavedEvent() {
            OutboxEvent event = createTestEvent();

            OutboxEvent saved = repository.save(event);

            assertThat(saved).isEqualTo(event);
        }

        @Test
        @DisplayName("rejects null event")
        void rejectsNullEvent() {
            assertThatThrownBy(() -> repository.save(null))
                    .isInstanceOf(NullPointerException.class);
        }
    }

    @Nested
    @DisplayName("saveAll")
    class SaveAllTest {

        @Test
        @DisplayName("persists multiple events")
        void persistsMultipleEvents() {
            List<OutboxEvent> events = List.of(
                    createTestEvent("order-1"),
                    createTestEvent("order-2")
            );

            repository.saveAll(events);

            assertThat(repository.size()).isEqualTo(2);
        }
    }

    @Nested
    @DisplayName("findById")
    class FindByIdTest {

        @Test
        @DisplayName("finds existing event")
        void findsExisting() {
            OutboxEvent event = createTestEvent();
            repository.save(event);

            Optional<OutboxEvent> found = repository.findById(event.id());

            assertThat(found).isPresent();
            assertThat(found.get().id()).isEqualTo(event.id());
        }

        @Test
        @DisplayName("returns empty for non-existent")
        void returnsEmptyForNonExistent() {
            Optional<OutboxEvent> found = repository.findById(OutboxEventId.generate());

            assertThat(found).isEmpty();
        }
    }

    @Nested
    @DisplayName("find")
    class FindTest {

        @BeforeEach
        void setupData() {
            repository.save(createTestEvent("order-1", "Order"));
            repository.save(createTestEvent("order-2", "Order"));
            repository.save(createTestEvent("payment-1", "Payment"));
        }

        @Test
        @DisplayName("finds pending events")
        void findsPending() {
            OutboxQuery query = OutboxQuery.pending(10);

            List<OutboxEvent> results = repository.find(query);

            assertThat(results).hasSize(3);
        }

        @Test
        @DisplayName("filters by aggregate type")
        void filtersByAggregateType() {
            OutboxQuery query = OutboxQuery.builder()
                    .aggregateType("Order")
                    .limit(10)
                    .build();

            List<OutboxEvent> results = repository.find(query);

            assertThat(results).hasSize(2);
            assertThat(results).allMatch(e -> e.aggregateType().equals("Order"));
        }

        @Test
        @DisplayName("respects limit")
        void respectsLimit() {
            OutboxQuery query = OutboxQuery.builder()
                    .limit(2)
                    .build();

            List<OutboxEvent> results = repository.find(query);

            assertThat(results).hasSize(2);
        }
    }

    @Nested
    @DisplayName("update")
    class UpdateTest {

        @Test
        @DisplayName("updates existing event")
        void updatesExisting() {
            OutboxEvent event = createTestEvent();
            repository.save(event);

            OutboxEvent updated = event.markPublished(Instant.now());
            repository.update(updated);

            Optional<OutboxEvent> found = repository.findById(event.id());
            assertThat(found).isPresent();
            assertThat(found.get().isPublished()).isTrue();
        }

        @Test
        @DisplayName("throws for non-existent event")
        void throwsForNonExistent() {
            OutboxEvent event = createTestEvent();

            assertThatThrownBy(() -> repository.update(event))
                    .isInstanceOf(OutboxPersistenceException.class);
        }
    }

    @Nested
    @DisplayName("updateStatus")
    class UpdateStatusTest {

        @Test
        @DisplayName("updates status for multiple events")
        void updatesMultiple() {
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
    }

    @Nested
    @DisplayName("deleteById")
    class DeleteByIdTest {

        @Test
        @DisplayName("deletes existing event")
        void deletesExisting() {
            OutboxEvent event = createTestEvent();
            repository.save(event);

            boolean deleted = repository.deleteById(event.id());

            assertThat(deleted).isTrue();
            assertThat(repository.findById(event.id())).isEmpty();
        }

        @Test
        @DisplayName("returns false for non-existent")
        void returnsFalseForNonExistent() {
            boolean deleted = repository.deleteById(OutboxEventId.generate());

            assertThat(deleted).isFalse();
        }
    }

    @Nested
    @DisplayName("count")
    class CountTest {

        @BeforeEach
        void setupData() {
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
        @DisplayName("counts by status")
        void countsByStatus() {
            assertThat(repository.count(OutboxStatusFilter.PENDING)).isEqualTo(2);
            assertThat(repository.count(OutboxStatusFilter.PUBLISHED)).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("utility methods")
    class UtilityMethodsTest {

        @Test
        @DisplayName("findAll returns all events")
        void findAllReturnsAll() {
            repository.save(createTestEvent("order-1"));
            repository.save(createTestEvent("order-2"));

            assertThat(repository.findAll()).hasSize(2);
        }

        @Test
        @DisplayName("clear removes all events")
        void clearRemovesAll() {
            repository.save(createTestEvent("order-1"));
            repository.save(createTestEvent("order-2"));

            repository.clear();

            assertThat(repository.isEmpty()).isTrue();
        }

        @Test
        @DisplayName("size returns event count")
        void sizeReturnsCount() {
            repository.save(createTestEvent());

            assertThat(repository.size()).isEqualTo(1);
        }

        @Test
        @DisplayName("isEmpty returns true when empty")
        void isEmptyReturnsTrue() {
            assertThat(repository.isEmpty()).isTrue();

            repository.save(createTestEvent());

            assertThat(repository.isEmpty()).isFalse();
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
                .payload("{\"test\":true}".getBytes(StandardCharsets.UTF_8))
                .build();
    }
}
