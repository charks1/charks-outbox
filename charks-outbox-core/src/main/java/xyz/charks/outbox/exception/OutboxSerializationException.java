package xyz.charks.outbox.exception;

import org.jspecify.annotations.Nullable;

/**
 * Exception thrown when serialization or deserialization of event payloads fails.
 *
 * <p>This exception is thrown by {@link xyz.charks.outbox.serializer.Serializer}
 * implementations when payload conversion fails. Common causes include:
 * <ul>
 *   <li>Invalid JSON/Avro/Protobuf structure</li>
 *   <li>Missing required fields</li>
 *   <li>Type mismatches</li>
 *   <li>Schema incompatibilities</li>
 * </ul>
 */
public class OutboxSerializationException extends OutboxException {

    /**
     * Creates an OutboxSerializationException with a message.
     *
     * @param message the error message
     */
    public OutboxSerializationException(String message) {
        super(message);
    }

    /**
     * Creates an OutboxSerializationException with a message and cause.
     *
     * @param message the error message
     * @param cause the underlying serialization exception
     */
    public OutboxSerializationException(String message, @Nullable Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates an OutboxSerializationException with a cause.
     *
     * @param cause the underlying serialization exception
     */
    public OutboxSerializationException(Throwable cause) {
        super(cause);
    }
}
