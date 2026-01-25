package xyz.charks.outbox.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@DisplayName("MongoOutboxRepository")
class MongoOutboxRepositoryTest {

    private MongoOutboxRepository repository;
    private MongoCollection<Document> collection;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        MongoClient client = mock(MongoClient.class);
        MongoDatabase database = mock(MongoDatabase.class);
        collection = mock(MongoCollection.class);

        when(client.getDatabase(anyString())).thenReturn(database);
        when(database.getCollection(anyString())).thenReturn(collection);

        MongoOutboxConfig config = MongoOutboxConfig.builder()
                .client(client)
                .database("testdb")
                .collection("test_outbox")
                .build();

        repository = new MongoOutboxRepository(config);
    }

    @Nested
    @DisplayName("constructor")
    class ConstructorTest {

        @Test
        @DisplayName("throws exception for null config")
        void nullConfig() {
            assertThatThrownBy(() -> new MongoOutboxRepository(null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("config");
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
