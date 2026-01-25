package xyz.charks.outbox.serializer.json;

import xyz.charks.outbox.exception.OutboxSerializationException;
import xyz.charks.outbox.serializer.Serializer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.util.Objects;

/**
 * Jackson-based JSON serializer implementation.
 *
 * <p>Provides JSON serialization with support for:
 * <ul>
 *   <li>Java 8+ date/time types via JavaTimeModule</li>
 *   <li>Configurable ObjectMapper for custom settings</li>
 *   <li>Pretty printing option for debugging</li>
 *   <li>Unknown property handling</li>
 * </ul>
 *
 * <p>Default configuration:
 * <ul>
 *   <li>ISO-8601 date format (no timestamps as numbers)</li>
 *   <li>Ignores unknown properties during deserialization</li>
 *   <li>Compact output (no pretty printing)</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * // With defaults
 * Serializer<Object> serializer = JacksonSerializer.create();
 *
 * // With custom ObjectMapper
 * ObjectMapper mapper = new ObjectMapper()
 *     .registerModule(new JavaTimeModule())
 *     .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
 * Serializer<Object> customSerializer = JacksonSerializer.create(mapper);
 *
 * // Usage
 * byte[] json = serializer.serialize(myEvent);
 * MyEvent restored = serializer.deserialize(json, MyEvent.class);
 * }</pre>
 *
 * @see Serializer
 */
public final class JacksonSerializer implements Serializer<Object> {

    private static final String CONTENT_TYPE = "application/json";

    private final ObjectMapper objectMapper;

    private JacksonSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * Creates a serializer with default configuration.
     *
     * <p>The default ObjectMapper is configured with:
     * <ul>
     *   <li>JavaTimeModule for date/time support</li>
     *   <li>ISO-8601 date format</li>
     *   <li>Ignore unknown properties</li>
     * </ul>
     *
     * @return a new JacksonSerializer with defaults
     */
    public static JacksonSerializer create() {
        return create(createDefaultObjectMapper());
    }

    /**
     * Creates a serializer with a custom ObjectMapper.
     *
     * <p>The provided ObjectMapper should be configured with appropriate
     * modules and settings for your use case. Consider registering
     * JavaTimeModule for date/time support.
     *
     * @param objectMapper the ObjectMapper to use
     * @return a new JacksonSerializer using the provided mapper
     * @throws NullPointerException if objectMapper is null
     */
    public static JacksonSerializer create(ObjectMapper objectMapper) {
        Objects.requireNonNull(objectMapper, "ObjectMapper cannot be null");
        return new JacksonSerializer(objectMapper);
    }

    /**
     * Creates a serializer with pretty printing enabled.
     *
     * <p>Useful for debugging and development. Not recommended for
     * production due to increased payload size.
     *
     * @return a new JacksonSerializer with pretty printing
     */
    public static JacksonSerializer prettyPrint() {
        ObjectMapper mapper = createDefaultObjectMapper()
                .enable(SerializationFeature.INDENT_OUTPUT);
        return new JacksonSerializer(mapper);
    }

    @Override
    public byte[] serialize(Object value) {
        Objects.requireNonNull(value, "Value cannot be null");
        try {
            return objectMapper.writeValueAsBytes(value);
        } catch (JsonProcessingException e) {
            throw new OutboxSerializationException(
                    "Failed to serialize value of type " + value.getClass().getName(), e);
        }
    }

    @Override
    public <R> R deserialize(byte[] data, Class<R> type) {
        Objects.requireNonNull(data, "Data cannot be null");
        Objects.requireNonNull(type, "Type cannot be null");
        try {
            return objectMapper.readValue(data, type);
        } catch (IOException e) {
            throw new OutboxSerializationException(
                    "Failed to deserialize value to type " + type.getName(), e);
        }
    }

    @Override
    public String contentType() {
        return CONTENT_TYPE;
    }

    /**
     * Returns the underlying ObjectMapper.
     *
     * <p>Use with caution - modifying the returned mapper affects
     * all future serialization operations.
     *
     * @return the ObjectMapper used by this serializer
     */
    public ObjectMapper objectMapper() {
        return objectMapper;
    }

    private static ObjectMapper createDefaultObjectMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }
}
