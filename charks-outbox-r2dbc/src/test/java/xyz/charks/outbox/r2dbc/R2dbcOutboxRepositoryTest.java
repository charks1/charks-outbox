package xyz.charks.outbox.r2dbc;

import io.r2dbc.spi.ConnectionFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import xyz.charks.outbox.core.AggregateId;
import xyz.charks.outbox.core.EventType;
import xyz.charks.outbox.core.OutboxEvent;
import xyz.charks.outbox.core.OutboxEventId;
import xyz.charks.outbox.core.OutboxQuery;
import xyz.charks.outbox.core.Published;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

@DisplayName("R2dbcOutboxRepository")
class R2dbcOutboxRepositoryTest {

    private R2dbcOutboxRepository repository;

    @BeforeEach
    void setUp() {
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        R2dbcOutboxConfig config = R2dbcOutboxConfig.builder()
                .connectionFactory(connectionFactory)
                .build();
        repository = new R2dbcOutboxRepository(config);
    }

    @Nested
    @DisplayName("constructor")
    class ConstructorTest {

        @Test
        @DisplayName("throws exception for null config")
        void nullConfig() {
            assertThatThrownBy(() -> new R2dbcOutboxRepository(null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("config");
        }

        @Test
        @DisplayName("creates repository with valid config")
        void validConfig() {
            ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
            R2dbcOutboxConfig config = R2dbcOutboxConfig.builder()
                    .connectionFactory(connectionFactory)
                    .build();

            R2dbcOutboxRepository repo = new R2dbcOutboxRepository(config);

            assertThat(repo).isNotNull();
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
                    .hasMessageContaining("event");
        }

        @Test
        @DisplayName("returns the saved event")
        void returnsSavedEvent() {
            OutboxEvent event = createTestEvent();

            OutboxEvent result = repository.save(event);

            assertThat(result).isEqualTo(event);
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
                    .hasMessageContaining("events");
        }

        @Test
        @DisplayName("returns empty list for empty input")
        void emptyList() {
            List<OutboxEvent> result = repository.saveAll(List.of());

            assertThat(result).isEmpty();
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
                    .hasMessageContaining("id");
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
                    .hasMessageContaining("query");
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
                    .hasMessageContaining("event");
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
                    .hasMessageContaining("ids");
        }

        @Test
        @DisplayName("throws exception for null status")
        void nullStatus() {
            assertThatThrownBy(() -> repository.updateStatus(List.of(), null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("status");
        }

        @Test
        @DisplayName("returns zero for empty list")
        void emptyList() {
            int result = repository.updateStatus(List.of(), Published.now());

            assertThat(result).isZero();
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
                    .hasMessageContaining("id");
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
                    .hasMessageContaining("query");
        }
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
