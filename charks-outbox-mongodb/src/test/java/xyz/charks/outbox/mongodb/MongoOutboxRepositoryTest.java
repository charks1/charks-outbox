package xyz.charks.outbox.mongodb;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import xyz.charks.outbox.core.*;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("MongoOutboxRepository")
class MongoOutboxRepositoryTest {

    @Mock
    private MongoClient client;

    @Mock
    private MongoDatabase database;

    @Mock
    private MongoCollection<Document> collection;

    @Mock
    private FindIterable<Document> findIterable;

    private MongoOutboxRepository repository;

    @BeforeEach
    void setUp() {
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

        @Test
        @DisplayName("inserts document and returns event")
        void savesEvent() {
            OutboxEvent event = createTestEvent();

            OutboxEvent result = repository.save(event);

            assertThat(result).isEqualTo(event);
            verify(collection).insertOne(any(Document.class));
        }

        @Test
        @DisplayName("converts event to document with correct fields")
        void convertsToDocument() {
            OutboxEvent event = createTestEvent();
            ArgumentCaptor<Document> docCaptor = ArgumentCaptor.forClass(Document.class);

            repository.save(event);

            verify(collection).insertOne(docCaptor.capture());
            Document doc = docCaptor.getValue();
            assertThat(doc.getString("_id")).isEqualTo(event.id().value().toString());
            assertThat(doc.getString("aggregateType")).isEqualTo("Order");
            assertThat(doc.getString("aggregateId")).isEqualTo("order-123");
            assertThat(doc.getString("eventType")).isEqualTo("OrderCreated");
            assertThat(doc.getString("topic")).isEqualTo("orders");
            assertThat(doc.getString("status")).isEqualTo("PENDING");
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

        @Test
        @DisplayName("inserts multiple documents")
        void savesMultipleEvents() {
            List<OutboxEvent> events = List.of(
                    createTestEvent("id-1"),
                    createTestEvent("id-2")
            );

            List<OutboxEvent> result = repository.saveAll(events);

            assertThat(result).hasSize(2);
            verify(collection).insertMany(any());
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

        @Test
        @DisplayName("returns empty when not found")
        void notFound() {
            when(collection.find(any(Bson.class))).thenReturn(findIterable);
            when(findIterable.first()).thenReturn(null);

            var result = repository.findById(OutboxEventId.generate());

            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("returns event when found")
        void found() {
            UUID eventId = UUID.randomUUID();
            Document doc = createTestDocument(eventId.toString(), "PENDING");
            when(collection.find(any(Bson.class))).thenReturn(findIterable);
            when(findIterable.first()).thenReturn(doc);

            var result = repository.findById(new OutboxEventId(eventId));

            assertThat(result).isPresent();
            assertThat(result.get().id().value()).isEqualTo(eventId);
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

        @Test
        @DisplayName("finds events with limit")
        @SuppressWarnings("unchecked")
        void findsWithLimit() {
            when(collection.find(any(Bson.class))).thenReturn(findIterable);
            when(findIterable.limit(10)).thenReturn(findIterable);
            when(findIterable.map(any())).thenReturn(mock(FindIterable.class));
            when(findIterable.map(any()).into(any())).thenReturn(new ArrayList<>());

            repository.find(OutboxQuery.pending(10));

            verify(findIterable).limit(10);
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

        @Test
        @DisplayName("replaces document")
        void updatesEvent() {
            OutboxEvent event = createTestEvent();
            UpdateResult updateResult = mock(UpdateResult.class);
            when(updateResult.getMatchedCount()).thenReturn(1L);
            when(collection.replaceOne(any(Bson.class), any(Document.class))).thenReturn(updateResult);

            repository.update(event);

            verify(collection).replaceOne(any(Bson.class), any(Document.class));
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

        @Test
        @DisplayName("updates multiple documents")
        void updatesMultiple() {
            UpdateResult updateResult = mock(UpdateResult.class);
            when(updateResult.getModifiedCount()).thenReturn(2L);
            when(collection.updateMany(any(Bson.class), any(Bson.class))).thenReturn(updateResult);

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
                    .hasMessageContaining("id");
        }

        @Test
        @DisplayName("returns true when deleted")
        void deleted() {
            DeleteResult deleteResult = mock(DeleteResult.class);
            when(deleteResult.getDeletedCount()).thenReturn(1L);
            when(collection.deleteOne(any(Bson.class))).thenReturn(deleteResult);

            boolean result = repository.deleteById(OutboxEventId.generate());

            assertThat(result).isTrue();
        }

        @Test
        @DisplayName("returns false when not found")
        void notFound() {
            DeleteResult deleteResult = mock(DeleteResult.class);
            when(deleteResult.getDeletedCount()).thenReturn(0L);
            when(collection.deleteOne(any(Bson.class))).thenReturn(deleteResult);

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
                    .hasMessageContaining("query");
        }

        @Test
        @DisplayName("deletes matching documents")
        void deletesMatching() {
            DeleteResult deleteResult = mock(DeleteResult.class);
            when(deleteResult.getDeletedCount()).thenReturn(5L);
            when(collection.deleteMany(any(Bson.class))).thenReturn(deleteResult);

            int result = repository.delete(OutboxQuery.pending(100));

            assertThat(result).isEqualTo(5);
        }
    }

    @Nested
    @DisplayName("count")
    class CountTest {

        @Test
        @DisplayName("counts all documents when no filter")
        void countsAll() {
            when(collection.countDocuments()).thenReturn(10L);

            long result = repository.count(null);

            assertThat(result).isEqualTo(10);
            verify(collection).countDocuments();
        }

        @Test
        @DisplayName("counts filtered documents")
        void countsFiltered() {
            when(collection.countDocuments(any(Bson.class))).thenReturn(5L);

            long result = repository.count(OutboxStatusFilter.PENDING);

            assertThat(result).isEqualTo(5);
        }
    }

    @Nested
    @DisplayName("document mapping")
    class DocumentMappingTest {

        @Test
        @DisplayName("maps PUBLISHED status correctly")
        void mapsPublishedStatus() {
            Document doc = createTestDocument(UUID.randomUUID().toString(), "PUBLISHED");
            when(collection.find(any(Bson.class))).thenReturn(findIterable);
            when(findIterable.first()).thenReturn(doc);

            var result = repository.findById(OutboxEventId.generate());

            assertThat(result).isPresent();
            assertThat(result.get().status()).isInstanceOf(Published.class);
        }

        @Test
        @DisplayName("maps FAILED status correctly")
        void mapsFailedStatus() {
            Document doc = createTestDocument(UUID.randomUUID().toString(), "FAILED");
            doc.put("lastError", "Connection timeout");
            when(collection.find(any(Bson.class))).thenReturn(findIterable);
            when(findIterable.first()).thenReturn(doc);

            var result = repository.findById(OutboxEventId.generate());

            assertThat(result).isPresent();
            assertThat(result.get().status()).isInstanceOf(Failed.class);
            assertThat(result.get().lastError()).isEqualTo("Connection timeout");
        }

        @Test
        @DisplayName("maps ARCHIVED status correctly")
        void mapsArchivedStatus() {
            Document doc = createTestDocument(UUID.randomUUID().toString(), "ARCHIVED");
            when(collection.find(any(Bson.class))).thenReturn(findIterable);
            when(findIterable.first()).thenReturn(doc);

            var result = repository.findById(OutboxEventId.generate());

            assertThat(result).isPresent();
            assertThat(result.get().status()).isInstanceOf(Archived.class);
        }

        @Test
        @DisplayName("defaults to PENDING for unknown status")
        void defaultsToPending() {
            Document doc = createTestDocument(UUID.randomUUID().toString(), "UNKNOWN");
            when(collection.find(any(Bson.class))).thenReturn(findIterable);
            when(findIterable.first()).thenReturn(doc);

            var result = repository.findById(OutboxEventId.generate());

            assertThat(result).isPresent();
            assertThat(result.get().status()).isInstanceOf(Pending.class);
        }

        @Test
        @DisplayName("handles null payload")
        void handlesNullPayload() {
            Document doc = createTestDocument(UUID.randomUUID().toString(), "PENDING");
            doc.remove("payload");
            when(collection.find(any(Bson.class))).thenReturn(findIterable);
            when(findIterable.first()).thenReturn(doc);

            var result = repository.findById(OutboxEventId.generate());

            assertThat(result).isPresent();
            assertThat(result.get().payload()).isEmpty();
        }

        @Test
        @DisplayName("handles null headers")
        void handlesNullHeaders() {
            Document doc = createTestDocument(UUID.randomUUID().toString(), "PENDING");
            doc.remove("headers");
            when(collection.find(any(Bson.class))).thenReturn(findIterable);
            when(findIterable.first()).thenReturn(doc);

            var result = repository.findById(OutboxEventId.generate());

            assertThat(result).isPresent();
            assertThat(result.get().headers()).isEmpty();
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

    private OutboxEvent createTestEvent(String aggregateId) {
        return OutboxEvent.builder()
                .aggregateType("Order")
                .aggregateId(AggregateId.of(aggregateId))
                .eventType(EventType.of("OrderCreated"))
                .topic("orders")
                .partitionKey(aggregateId)
                .payload("{\"test\":true}".getBytes(StandardCharsets.UTF_8))
                .build();
    }

    private Document createTestDocument(String id, String status) {
        Document doc = new Document();
        doc.put("_id", id);
        doc.put("aggregateType", "Order");
        doc.put("aggregateId", "order-123");
        doc.put("eventType", "OrderCreated");
        doc.put("topic", "orders");
        doc.put("partitionKey", "order-123");
        doc.put("payload", new Binary("{\"test\":true}".getBytes(StandardCharsets.UTF_8)));
        doc.put("headers", Map.of());
        doc.put("status", status);
        doc.put("retryCount", 0);
        doc.put("createdAt", Date.from(Instant.now()));
        doc.put("processedAt", Date.from(Instant.now()));
        return doc;
    }
}
