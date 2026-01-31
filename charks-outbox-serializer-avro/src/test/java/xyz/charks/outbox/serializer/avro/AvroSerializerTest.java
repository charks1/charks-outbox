package xyz.charks.outbox.serializer.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import xyz.charks.outbox.exception.OutboxSerializationException;
import xyz.charks.outbox.serializer.avro.test.TestEvent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("AvroSerializer")
class AvroSerializerTest {

    private static final String TEST_SCHEMA_JSON = """
            {
                "type": "record",
                "name": "TestRecord",
                "namespace": "xyz.charks.outbox.test",
                "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "value", "type": "int"}
                ]
            }
            """;

    private AvroSerializer serializer;
    private Schema testSchema;

    @BeforeEach
    void setUp() {
        serializer = new AvroSerializer();
        testSchema = new Schema.Parser().parse(TEST_SCHEMA_JSON);
    }

    @Nested
    @DisplayName("contentType")
    class ContentTypeTest {

        @Test
        @DisplayName("returns application/avro")
        void returnsAvroContentType() {
            assertThat(serializer.contentType()).isEqualTo("application/avro");
        }
    }

    @Nested
    @DisplayName("serialize")
    class SerializeTest {

        @Test
        @DisplayName("throws exception for null payload")
        @SuppressWarnings("DataFlowIssue") // intentionally passing null to test rejection
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
        @DisplayName("serializes GenericRecord")
        void serializesGenericRecord() {
            GenericRecord avroRecord = new GenericData.Record(testSchema);
            avroRecord.put("id", "test-123");
            avroRecord.put("value", 42);

            byte[] result = serializer.serialize(avroRecord);

            assertThat(result).isNotEmpty();
        }

        @Test
        @DisplayName("serializes SpecificRecordBase")
        void serializesSpecificRecord() {
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
    }

    @Nested
    @DisplayName("deserialize")
    class DeserializeTest {

        @Test
        @DisplayName("throws exception for null data")
        @SuppressWarnings("DataFlowIssue") // intentionally passing null to test rejection
        void nullData() {
            assertThatThrownBy(() -> serializer.deserialize(null, byte[].class))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("data");
        }

        @Test
        @DisplayName("throws exception for null type")
        @SuppressWarnings("DataFlowIssue") // intentionally passing null to test rejection
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
        @DisplayName("throws exception for unsupported type")
        void unsupportedType() {
            assertThatThrownBy(() -> serializer.deserialize(new byte[0], String.class))
                    .isInstanceOf(OutboxSerializationException.class)
                    .hasMessageContaining("Unsupported target type");
        }

        @Test
        @DisplayName("deserializes SpecificRecordBase")
        void deserializesSpecificRecord() {
            TestEvent original = TestEvent.newBuilder()
                    .setEventId("evt-002")
                    .setAggregateId("order-456")
                    .setEventType("OrderShipped")
                    .setTimestamp(1234567890L)
                    .setPayload(null)
                    .build();

            byte[] serialized = serializer.serialize(original);
            TestEvent result = serializer.deserialize(serialized, TestEvent.class);

            assertThat(result.getEventId()).isEqualTo("evt-002");
            assertThat(result.getAggregateId()).isEqualTo("order-456");
            assertThat(result.getEventType()).isEqualTo("OrderShipped");
            assertThat(result.getTimestamp()).isEqualTo(1234567890L);
            assertThat(result.getPayload()).isNull();
        }

        @Test
        @DisplayName("throws exception for invalid SpecificRecordBase data")
        void invalidSpecificRecordData() {
            byte[] invalidData = "not valid avro".getBytes();

            assertThatThrownBy(() -> serializer.deserialize(invalidData, TestEvent.class))
                    .isInstanceOf(OutboxSerializationException.class)
                    .hasMessageContaining("Failed to deserialize");
        }
    }

    @Nested
    @DisplayName("deserializeGeneric")
    class DeserializeGenericTest {

        @Test
        @DisplayName("throws exception for null data")
        @SuppressWarnings("DataFlowIssue") // intentionally passing null to test rejection
        void nullData() {
            assertThatThrownBy(() -> serializer.deserializeGeneric(null, testSchema))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("data");
        }

        @Test
        @DisplayName("throws exception for null schema")
        @SuppressWarnings("DataFlowIssue") // intentionally passing null to test rejection
        void nullSchema() {
            assertThatThrownBy(() -> serializer.deserializeGeneric(new byte[0], null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("schema");
        }

        @Test
        @DisplayName("deserializes generic record")
        void deserializesGenericRecord() {
            GenericRecord original = new GenericData.Record(testSchema);
            original.put("id", "test-123");
            original.put("value", 42);

            byte[] serialized = serializer.serialize(original);
            GenericRecord deserialized = serializer.deserializeGeneric(serialized, testSchema);

            assertThat(deserialized.get("id")).hasToString("test-123");
            assertThat(deserialized.get("value")).isEqualTo(42);
        }

        @Test
        @DisplayName("throws exception for invalid data")
        void invalidData() {
            byte[] invalidData = "not valid avro".getBytes();

            assertThatThrownBy(() -> serializer.deserializeGeneric(invalidData, testSchema))
                    .isInstanceOf(OutboxSerializationException.class)
                    .hasMessageContaining("Failed to deserialize");
        }
    }

    @Nested
    @DisplayName("round trip")
    class RoundTripTest {

        @Test
        @DisplayName("serializes and deserializes GenericRecord correctly")
        void roundTripGenericRecord() {
            GenericRecord original = new GenericData.Record(testSchema);
            original.put("id", "order-456");
            original.put("value", 100);

            byte[] serialized = serializer.serialize(original);
            GenericRecord deserialized = serializer.deserializeGeneric(serialized, testSchema);

            assertThat(deserialized.get("id")).hasToString("order-456");
            assertThat(deserialized.get("value")).isEqualTo(100);
        }

        @Test
        @DisplayName("preserves data through multiple round trips")
        void multipleRoundTrips() {
            GenericRecord original = new GenericData.Record(testSchema);
            original.put("id", "multi-trip");
            original.put("value", 999);

            byte[] serialized1 = serializer.serialize(original);
            GenericRecord deserialized1 = serializer.deserializeGeneric(serialized1, testSchema);
            byte[] serialized2 = serializer.serialize(deserialized1);
            GenericRecord deserialized2 = serializer.deserializeGeneric(serialized2, testSchema);

            assertThat(deserialized2.get("id")).hasToString("multi-trip");
            assertThat(deserialized2.get("value")).isEqualTo(999);
        }

        @Test
        @DisplayName("round trips SpecificRecordBase correctly")
        void roundTripSpecificRecord() {
            TestEvent original = TestEvent.newBuilder()
                    .setEventId("evt-round")
                    .setAggregateId("agg-789")
                    .setEventType("AggregateUpdated")
                    .setTimestamp(9876543210L)
                    .setPayload("{\"key\": \"value\"}")
                    .build();

            byte[] serialized = serializer.serialize(original);
            TestEvent deserialized = serializer.deserialize(serialized, TestEvent.class);

            assertThat(deserialized.getEventId()).isEqualTo("evt-round");
            assertThat(deserialized.getAggregateId()).isEqualTo("agg-789");
            assertThat(deserialized.getEventType()).isEqualTo("AggregateUpdated");
            assertThat(deserialized.getTimestamp()).isEqualTo(9876543210L);
            assertThat(deserialized.getPayload()).isEqualTo("{\"key\": \"value\"}");
        }

        @Test
        @DisplayName("SpecificRecordBase survives multiple round trips")
        void multipleRoundTripsSpecificRecord() {
            TestEvent original = TestEvent.newBuilder()
                    .setEventId("evt-multi")
                    .setAggregateId("agg-multi")
                    .setEventType("MultiTrip")
                    .setTimestamp(1111111111L)
                    .setPayload("round-trip-data")
                    .build();

            byte[] serialized1 = serializer.serialize(original);
            TestEvent deserialized1 = serializer.deserialize(serialized1, TestEvent.class);
            byte[] serialized2 = serializer.serialize(deserialized1);
            TestEvent deserialized2 = serializer.deserialize(serialized2, TestEvent.class);

            assertThat(deserialized2.getEventId()).isEqualTo("evt-multi");
            assertThat(deserialized2.getAggregateId()).isEqualTo("agg-multi");
            assertThat(deserialized2.getEventType()).isEqualTo("MultiTrip");
            assertThat(deserialized2.getTimestamp()).isEqualTo(1111111111L);
            assertThat(deserialized2.getPayload()).isEqualTo("round-trip-data");
        }
    }
}
