package xyz.charks.outbox.serializer.avro;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import xyz.charks.outbox.exception.OutboxSerializationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("AvroSerializer")
class AvroSerializerTest {

    private AvroSerializer serializer;

    @BeforeEach
    void setUp() {
        serializer = new AvroSerializer();
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
        @DisplayName("throws exception for unsupported type")
        void unsupportedType() {
            assertThatThrownBy(() -> serializer.deserialize(new byte[0], String.class))
                    .isInstanceOf(OutboxSerializationException.class)
                    .hasMessageContaining("Unsupported target type");
        }
    }
}
