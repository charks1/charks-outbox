package xyz.charks.outbox.serializer.protobuf;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import xyz.charks.outbox.exception.OutboxSerializationException;
import xyz.charks.outbox.serializer.protobuf.test.TestEvent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("ProtobufSerializer")
class ProtobufSerializerTest {

    private ProtobufSerializer serializer;

    @BeforeEach
    void setUp() {
        serializer = new ProtobufSerializer();
    }

    @Nested
    @DisplayName("contentType")
    class ContentTypeTest {

        @Test
        @DisplayName("returns application/x-protobuf")
        void returnsProtobufContentType() {
            assertThat(serializer.contentType()).isEqualTo("application/x-protobuf");
        }
    }

    @Nested
    @DisplayName("serialize")
    class SerializeTest {

        @Test
        @DisplayName("throws exception for null payload")
        void nullPayload() {
            assertThatThrownBy(() -> serializer.serialize(null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("payload");
        }

        @Test
        @DisplayName("returns byte array unchanged")
        void serializesByteArray() {
            byte[] input = "test data".getBytes();

            byte[] result = serializer.serialize(input);

            assertThat(result).isEqualTo(input);
        }

        @Test
        @DisplayName("throws exception for unsupported type")
        void unsupportedType() {
            assertThatThrownBy(() -> serializer.serialize("plain string"))
                    .isInstanceOf(OutboxSerializationException.class)
                    .hasMessageContaining("Unsupported payload type");
        }

        @Test
        @DisplayName("serializes Protobuf Message")
        void serializesMessage() {
            TestEvent event = TestEvent.newBuilder()
                    .setEventId("evt-001")
                    .setAggregateId("order-123")
                    .setEventType("OrderCreated")
                    .setTimestamp(System.currentTimeMillis())
                    .setPayload("{\"amount\": 100}")
                    .build();

            byte[] result = serializer.serialize(event);

            assertThat(result).isNotEmpty();
        }

        @Test
        @DisplayName("serializes Message without optional field")
        void serializesMessageWithoutOptional() {
            TestEvent event = TestEvent.newBuilder()
                    .setEventId("evt-002")
                    .setAggregateId("order-456")
                    .setEventType("OrderUpdated")
                    .setTimestamp(1234567890L)
                    .build();

            byte[] result = serializer.serialize(event);

            assertThat(result).isNotEmpty();
        }
    }

    @Nested
    @DisplayName("deserialize")
    class DeserializeTest {

        @Test
        @DisplayName("throws exception for null data")
        void nullData() {
            assertThatThrownBy(() -> serializer.deserialize(null, byte[].class))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("data");
        }

        @Test
        @DisplayName("throws exception for null type")
        void nullType() {
            assertThatThrownBy(() -> serializer.deserialize(new byte[0], null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("type");
        }

        @Test
        @DisplayName("returns byte array unchanged")
        void deserializesByteArray() {
            byte[] input = "test data".getBytes();

            byte[] result = serializer.deserialize(input, byte[].class);

            assertThat(result).isEqualTo(input);
        }

        @Test
        @DisplayName("throws exception for non-Message type")
        void unsupportedType() {
            assertThatThrownBy(() -> serializer.deserialize(new byte[0], String.class))
                    .isInstanceOf(OutboxSerializationException.class)
                    .hasMessageContaining("Unsupported target type");
        }

        @Test
        @DisplayName("deserializes Protobuf Message")
        void deserializesMessage() {
            TestEvent original = TestEvent.newBuilder()
                    .setEventId("evt-003")
                    .setAggregateId("order-789")
                    .setEventType("OrderShipped")
                    .setTimestamp(9876543210L)
                    .setPayload("{\"tracking\": \"ABC123\"}")
                    .build();

            byte[] serialized = serializer.serialize(original);
            TestEvent result = serializer.deserialize(serialized, TestEvent.class);

            assertThat(result.getEventId()).isEqualTo("evt-003");
            assertThat(result.getAggregateId()).isEqualTo("order-789");
            assertThat(result.getEventType()).isEqualTo("OrderShipped");
            assertThat(result.getTimestamp()).isEqualTo(9876543210L);
            assertThat(result.getPayload()).isEqualTo("{\"tracking\": \"ABC123\"}");
        }

        @Test
        @DisplayName("throws exception for invalid data")
        void invalidData() {
            byte[] invalidData = "not valid protobuf".getBytes();

            assertThatThrownBy(() -> serializer.deserialize(invalidData, TestEvent.class))
                    .isInstanceOf(OutboxSerializationException.class)
                    .hasMessageContaining("Failed to deserialize");
        }
    }

    @Nested
    @DisplayName("round trip")
    class RoundTripTest {

        @Test
        @DisplayName("serializes and deserializes Message correctly")
        void roundTripMessage() {
            TestEvent original = TestEvent.newBuilder()
                    .setEventId("evt-round")
                    .setAggregateId("agg-789")
                    .setEventType("AggregateUpdated")
                    .setTimestamp(1111111111L)
                    .setPayload("{\"key\": \"value\"}")
                    .build();

            byte[] serialized = serializer.serialize(original);
            TestEvent deserialized = serializer.deserialize(serialized, TestEvent.class);

            assertThat(deserialized).isEqualTo(original);
        }

        @Test
        @DisplayName("Message survives multiple round trips")
        void multipleRoundTrips() {
            TestEvent original = TestEvent.newBuilder()
                    .setEventId("evt-multi")
                    .setAggregateId("agg-multi")
                    .setEventType("MultiTrip")
                    .setTimestamp(2222222222L)
                    .setPayload("round-trip-data")
                    .build();

            byte[] serialized1 = serializer.serialize(original);
            TestEvent deserialized1 = serializer.deserialize(serialized1, TestEvent.class);
            byte[] serialized2 = serializer.serialize(deserialized1);
            TestEvent deserialized2 = serializer.deserialize(serialized2, TestEvent.class);

            assertThat(deserialized2).isEqualTo(original);
        }

        @Test
        @DisplayName("handles Message without optional fields")
        void roundTripWithoutOptional() {
            TestEvent original = TestEvent.newBuilder()
                    .setEventId("evt-no-opt")
                    .setAggregateId("agg-no-opt")
                    .setEventType("NoOptional")
                    .setTimestamp(3333333333L)
                    .build();

            byte[] serialized = serializer.serialize(original);
            TestEvent deserialized = serializer.deserialize(serialized, TestEvent.class);

            assertThat(deserialized.getEventId()).isEqualTo("evt-no-opt");
            assertThat(deserialized.hasPayload()).isFalse();
        }
    }

    @Nested
    @DisplayName("parser caching")
    class ParserCachingTest {

        @Test
        @DisplayName("reuses parser for same type")
        void reusesParser() {
            TestEvent event = TestEvent.newBuilder()
                    .setEventId("evt-cache")
                    .setAggregateId("agg-cache")
                    .setEventType("CacheTest")
                    .setTimestamp(4444444444L)
                    .build();

            byte[] serialized = serializer.serialize(event);

            // Deserialize multiple times - parser should be cached
            TestEvent result1 = serializer.deserialize(serialized, TestEvent.class);
            TestEvent result2 = serializer.deserialize(serialized, TestEvent.class);
            TestEvent result3 = serializer.deserialize(serialized, TestEvent.class);

            assertThat(result1).isEqualTo(event);
            assertThat(result2).isEqualTo(event);
            assertThat(result3).isEqualTo(event);
        }
    }
}
