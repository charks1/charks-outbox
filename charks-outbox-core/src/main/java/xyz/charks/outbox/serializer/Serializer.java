package xyz.charks.outbox.serializer;

import xyz.charks.outbox.exception.OutboxSerializationException;

/**
 * Interface for serializing and deserializing event payloads.
 *
 * <p>Implementations handle the conversion between Java objects and byte arrays
 * for storage in the outbox table and transmission to message brokers.
 *
 * <p>Common implementations:
 * <ul>
 *   <li>JSON using Jackson</li>
 *   <li>Apache Avro</li>
 *   <li>Protocol Buffers</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * Serializer serializer = new JacksonSerializer();
 *
 * // Serialize for storage
 * OrderCreatedEvent domainEvent = new OrderCreatedEvent(orderId, items);
 * byte[] payload = serializer.serialize(domainEvent);
 *
 * // Deserialize when needed
 * OrderCreatedEvent restored = serializer.deserialize(payload, OrderCreatedEvent.class);
 * }</pre>
 *
 * @param <T> the base type this serializer handles (use Object for generic serializers)
 */
public interface Serializer<T> {

    /**
     * Serializes an object to a byte array.
     *
     * @param value the object to serialize
     * @return the serialized bytes
     * @throws OutboxSerializationException if serialization fails
     * @throws NullPointerException if value is null
     */
    byte[] serialize(T value);

    /**
     * Deserializes a byte array to an object of the specified type.
     *
     * @param data the bytes to deserialize
     * @param type the target class
     * @param <R> the target type
     * @return the deserialized object
     * @throws OutboxSerializationException if deserialization fails
     * @throws NullPointerException if data or type is null
     */
    <R extends T> R deserialize(byte[] data, Class<R> type);

    /**
     * Returns the content type for this serializer.
     *
     * <p>This value can be stored as a header for consumers to determine
     * how to deserialize the payload.
     *
     * @return the MIME content type (e.g., "application/json", "application/avro")
     */
    String contentType();
}
