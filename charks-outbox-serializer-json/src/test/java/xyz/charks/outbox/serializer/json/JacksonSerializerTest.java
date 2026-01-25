package xyz.charks.outbox.serializer.json;

import xyz.charks.outbox.exception.OutboxSerializationException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("JacksonSerializer")
class JacksonSerializerTest {

    private JacksonSerializer serializer;

    @BeforeEach
    void setup() {
        serializer = JacksonSerializer.create();
    }

    @Nested
    @DisplayName("serialize")
    class SerializeTest {

        @Test
        @DisplayName("serializes simple object to JSON")
        void serializesSimpleObject() {
            TestEvent event = new TestEvent("test-123", "Hello");

            byte[] result = serializer.serialize(event);

            String json = new String(result, StandardCharsets.UTF_8);
            assertThat(json).contains("\"id\":\"test-123\"");
            assertThat(json).contains("\"message\":\"Hello\"");
        }

        @Test
        @DisplayName("serializes nested objects")
        void serializesNestedObjects() {
            NestedEvent event = new NestedEvent(
                    "order-1",
                    new Address("123 Main St", "New York")
            );

            byte[] result = serializer.serialize(event);

            String json = new String(result, StandardCharsets.UTF_8);
            assertThat(json).contains("\"orderId\":\"order-1\"");
            assertThat(json).contains("\"street\":\"123 Main St\"");
            assertThat(json).contains("\"city\":\"New York\"");
        }

        @Test
        @DisplayName("serializes collections")
        void serializesCollections() {
            ListEvent event = new ListEvent(List.of("a", "b", "c"));

            byte[] result = serializer.serialize(event);

            String json = new String(result, StandardCharsets.UTF_8);
            assertThat(json).contains("[\"a\",\"b\",\"c\"]");
        }

        @Test
        @DisplayName("serializes maps")
        void serializesMaps() {
            MapEvent event = new MapEvent(Map.of("key1", "value1", "key2", "value2"));

            byte[] result = serializer.serialize(event);

            String json = new String(result, StandardCharsets.UTF_8);
            assertThat(json).contains("\"key1\":\"value1\"");
            assertThat(json).contains("\"key2\":\"value2\"");
        }

        @Test
        @DisplayName("serializes Java time types in ISO format")
        void serializesJavaTimeTypes() {
            TimeEvent event = new TimeEvent(
                    Instant.parse("2024-01-15T10:30:00Z"),
                    LocalDate.of(2024, 1, 15),
                    LocalDateTime.of(2024, 1, 15, 10, 30, 0)
            );

            byte[] result = serializer.serialize(event);

            String json = new String(result, StandardCharsets.UTF_8);
            assertThat(json).contains("2024-01-15T10:30:00Z");
            assertThat(json).contains("2024-01-15");
        }

        @Test
        @DisplayName("rejects null value")
        void rejectsNullValue() {
            assertThatThrownBy(() -> serializer.serialize(null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("null");
        }
    }

    @Nested
    @DisplayName("deserialize")
    class DeserializeTest {

        @Test
        @DisplayName("deserializes JSON to object")
        void deserializesToObject() {
            String json = "{\"id\":\"test-456\",\"message\":\"World\"}";

            TestEvent result = serializer.deserialize(json.getBytes(StandardCharsets.UTF_8), TestEvent.class);

            assertThat(result.id()).isEqualTo("test-456");
            assertThat(result.message()).isEqualTo("World");
        }

        @Test
        @DisplayName("deserializes nested objects")
        void deserializesNestedObjects() {
            String json = "{\"orderId\":\"order-2\",\"address\":{\"street\":\"456 Oak Ave\",\"city\":\"Boston\"}}";

            NestedEvent result = serializer.deserialize(json.getBytes(StandardCharsets.UTF_8), NestedEvent.class);

            assertThat(result.orderId()).isEqualTo("order-2");
            assertThat(result.address().street()).isEqualTo("456 Oak Ave");
            assertThat(result.address().city()).isEqualTo("Boston");
        }

        @Test
        @DisplayName("ignores unknown properties by default")
        void ignoresUnknownProperties() {
            String json = "{\"id\":\"test-789\",\"message\":\"Test\",\"unknownField\":\"ignored\"}";

            TestEvent result = serializer.deserialize(json.getBytes(StandardCharsets.UTF_8), TestEvent.class);

            assertThat(result.id()).isEqualTo("test-789");
            assertThat(result.message()).isEqualTo("Test");
        }

        @Test
        @DisplayName("deserializes Java time types")
        void deserializesJavaTimeTypes() {
            String json = "{\"instant\":\"2024-01-15T10:30:00Z\",\"date\":\"2024-01-15\",\"dateTime\":\"2024-01-15T10:30:00\"}";

            TimeEvent result = serializer.deserialize(json.getBytes(StandardCharsets.UTF_8), TimeEvent.class);

            assertThat(result.instant()).isEqualTo(Instant.parse("2024-01-15T10:30:00Z"));
            assertThat(result.date()).isEqualTo(LocalDate.of(2024, 1, 15));
            assertThat(result.dateTime()).isEqualTo(LocalDateTime.of(2024, 1, 15, 10, 30, 0));
        }

        @Test
        @DisplayName("throws exception on invalid JSON")
        void throwsOnInvalidJson() {
            byte[] invalidJson = "not valid json".getBytes(StandardCharsets.UTF_8);

            assertThatThrownBy(() -> serializer.deserialize(invalidJson, TestEvent.class))
                    .isInstanceOf(OutboxSerializationException.class)
                    .hasMessageContaining("deserialize");
        }

        @Test
        @DisplayName("rejects null data")
        void rejectsNullData() {
            assertThatThrownBy(() -> serializer.deserialize(null, TestEvent.class))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        @DisplayName("rejects null type")
        void rejectsNullType() {
            byte[] data = "{}".getBytes(StandardCharsets.UTF_8);

            assertThatThrownBy(() -> serializer.deserialize(data, null))
                    .isInstanceOf(NullPointerException.class);
        }
    }

    @Nested
    @DisplayName("roundtrip")
    class RoundtripTest {

        @Test
        @DisplayName("serializes and deserializes correctly")
        void roundtrip() {
            TestEvent original = new TestEvent("round-trip", "Testing roundtrip");

            byte[] serialized = serializer.serialize(original);
            TestEvent deserialized = serializer.deserialize(serialized, TestEvent.class);

            assertThat(deserialized).isEqualTo(original);
        }

        @Test
        @DisplayName("preserves Java time types")
        void preservesTimeTypes() {
            TimeEvent original = new TimeEvent(
                    Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS),
                    LocalDate.now(),
                    LocalDateTime.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS)
            );

            byte[] serialized = serializer.serialize(original);
            TimeEvent deserialized = serializer.deserialize(serialized, TimeEvent.class);

            assertThat(deserialized).isEqualTo(original);
        }
    }

    @Nested
    @DisplayName("factory methods")
    class FactoryMethodsTest {

        @Test
        @DisplayName("create() returns serializer with defaults")
        void createWithDefaults() {
            JacksonSerializer s = JacksonSerializer.create();

            assertThat(s).isNotNull();
            assertThat(s.objectMapper()).isNotNull();
            assertThat(s.contentType()).isEqualTo("application/json");
        }

        @Test
        @DisplayName("create(ObjectMapper) uses provided mapper")
        void createWithCustomMapper() {
            ObjectMapper customMapper = new ObjectMapper();

            JacksonSerializer s = JacksonSerializer.create(customMapper);

            assertThat(s.objectMapper()).isSameAs(customMapper);
        }

        @Test
        @DisplayName("create(ObjectMapper) rejects null")
        void createRejectsNull() {
            assertThatThrownBy(() -> JacksonSerializer.create(null))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        @DisplayName("prettyPrint() returns serializer with indentation")
        void prettyPrint() {
            JacksonSerializer s = JacksonSerializer.prettyPrint();
            TestEvent event = new TestEvent("pretty", "test");

            String json = new String(s.serialize(event), StandardCharsets.UTF_8);

            assertThat(json).contains("\n");
            assertThat(json).contains("  ");
        }
    }

    @Nested
    @DisplayName("contentType")
    class ContentTypeTest {

        @Test
        @DisplayName("returns application/json")
        void returnsJsonContentType() {
            assertThat(serializer.contentType()).isEqualTo("application/json");
        }
    }

    // Test records
    record TestEvent(String id, String message) {}

    record Address(String street, String city) {}

    record NestedEvent(String orderId, Address address) {}

    record ListEvent(List<String> items) {}

    record MapEvent(Map<String, String> data) {}

    record TimeEvent(Instant instant, LocalDate date, LocalDateTime dateTime) {}
}
