package xyz.charks.outbox.core;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class HeadersCodecTest {

    @Nested
    class SerializeTest {

        @Test
        void shouldSerializeEmptyMap() {
            String json = HeadersCodec.serialize(Map.of());

            assertThat(json).isEqualTo("{}");
        }

        @Test
        void shouldSerializeSingleEntry() {
            String json = HeadersCodec.serialize(Map.of("key", "value"));

            assertThat(json).isEqualTo("{\"key\":\"value\"}");
        }

        @Test
        void shouldSerializeMultipleEntries() {
            Map<String, String> headers = new LinkedHashMap<>();
            headers.put("key1", "value1");
            headers.put("key2", "value2");

            String json = HeadersCodec.serialize(headers);

            assertThat(json).isEqualTo("{\"key1\":\"value1\",\"key2\":\"value2\"}");
        }

        @Test
        void shouldEscapeQuotes() {
            String json = HeadersCodec.serialize(Map.of("key", "value\"quoted\""));

            assertThat(json).contains("\\\"quoted\\\"");
        }

        @Test
        void shouldEscapeBackslashes() {
            String json = HeadersCodec.serialize(Map.of("key", "path\\to\\file"));

            assertThat(json).contains("path\\\\to\\\\file");
        }

        @Test
        void shouldEscapeNewlines() {
            String json = HeadersCodec.serialize(Map.of("key", "line1\nline2"));

            assertThat(json).contains("line1\\nline2");
        }

        @Test
        void shouldEscapeTabs() {
            String json = HeadersCodec.serialize(Map.of("key", "col1\tcol2"));

            assertThat(json).contains("col1\\tcol2");
        }

        @Test
        void shouldEscapeCarriageReturns() {
            String json = HeadersCodec.serialize(Map.of("key", "line1\rline2"));

            assertThat(json).contains("line1\\rline2");
        }
    }

    @Nested
    class DeserializeTest {

        @Test
        void shouldDeserializeNull() {
            Map<String, String> headers = HeadersCodec.deserialize(null);

            assertThat(headers).isEmpty();
        }

        @Test
        void shouldDeserializeEmptyString() {
            Map<String, String> headers = HeadersCodec.deserialize("");

            assertThat(headers).isEmpty();
        }

        @Test
        void shouldDeserializeBlankString() {
            Map<String, String> headers = HeadersCodec.deserialize("   ");

            assertThat(headers).isEmpty();
        }

        @Test
        void shouldDeserializeEmptyObject() {
            Map<String, String> headers = HeadersCodec.deserialize("{}");

            assertThat(headers).isEmpty();
        }

        @Test
        void shouldDeserializeSingleEntry() {
            Map<String, String> headers = HeadersCodec.deserialize("{\"key\":\"value\"}");

            assertThat(headers).containsEntry("key", "value");
        }

        @Test
        void shouldDeserializeMultipleEntries() {
            Map<String, String> headers = HeadersCodec.deserialize("{\"key1\":\"value1\",\"key2\":\"value2\"}");

            assertThat(headers)
                    .containsEntry("key1", "value1")
                    .containsEntry("key2", "value2");
        }

        @Test
        void shouldDeserializeWithWhitespace() {
            Map<String, String> headers = HeadersCodec.deserialize("{ \"key\" : \"value\" }");

            assertThat(headers).containsEntry("key", "value");
        }

        @Test
        void shouldDeserializeEscapedQuotes() {
            Map<String, String> headers = HeadersCodec.deserialize("{\"key\":\"value\\\"quoted\\\"\"}");

            assertThat(headers).containsEntry("key", "value\"quoted\"");
        }

        @Test
        void shouldDeserializeEscapedBackslashes() {
            Map<String, String> headers = HeadersCodec.deserialize("{\"key\":\"path\\\\to\\\\file\"}");

            assertThat(headers).containsEntry("key", "path\\to\\file");
        }

        @Test
        void shouldDeserializeEscapedNewlines() {
            Map<String, String> headers = HeadersCodec.deserialize("{\"key\":\"line1\\nline2\"}");

            assertThat(headers).containsEntry("key", "line1\nline2");
        }

        @Test
        void shouldDeserializeEscapedTabs() {
            Map<String, String> headers = HeadersCodec.deserialize("{\"key\":\"col1\\tcol2\"}");

            assertThat(headers).containsEntry("key", "col1\tcol2");
        }

        @Test
        void shouldDeserializeEscapedCarriageReturns() {
            Map<String, String> headers = HeadersCodec.deserialize("{\"key\":\"line1\\rline2\"}");

            assertThat(headers).containsEntry("key", "line1\rline2");
        }

        @Test
        void shouldReturnImmutableMap() {
            Map<String, String> headers = HeadersCodec.deserialize("{\"key\":\"value\"}");

            assertThat(headers).isUnmodifiable();
        }
    }

    @Nested
    class RoundTripTest {

        @Test
        void shouldRoundTripEmptyMap() {
            Map<String, String> original = Map.of();

            String json = HeadersCodec.serialize(original);
            Map<String, String> restored = HeadersCodec.deserialize(json);

            assertThat(restored).isEqualTo(original);
        }

        @Test
        void shouldRoundTripSimpleHeaders() {
            Map<String, String> original = Map.of(
                    "content-type", "application/json",
                    "trace-id", "abc123"
            );

            String json = HeadersCodec.serialize(original);
            Map<String, String> restored = HeadersCodec.deserialize(json);

            assertThat(restored).isEqualTo(original);
        }

        @Test
        void shouldRoundTripHeadersWithSpecialCharacters() {
            Map<String, String> original = Map.of(
                    "message", "Hello \"World\"!\nLine 2\tTabbed",
                    "path", "C:\\Users\\test"
            );

            String json = HeadersCodec.serialize(original);
            Map<String, String> restored = HeadersCodec.deserialize(json);

            assertThat(restored).isEqualTo(original);
        }
    }
}
