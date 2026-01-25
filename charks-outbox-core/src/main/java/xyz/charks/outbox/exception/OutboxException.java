package xyz.charks.outbox.exception;

import org.jspecify.annotations.Nullable;

/**
 * Base exception for all outbox-related errors.
 *
 * <p>This is the root of the outbox exception hierarchy. Catch this type
 * to handle any outbox operation failure in a generic way.
 *
 * @see OutboxPersistenceException
 * @see OutboxPublishException
 * @see OutboxSerializationException
 */
public class OutboxException extends RuntimeException {

    /**
     * Creates an OutboxException with a message.
     *
     * @param message the error message
     */
    public OutboxException(String message) {
        super(message);
    }

    /**
     * Creates an OutboxException with a message and cause.
     *
     * @param message the error message
     * @param cause the underlying cause
     */
    public OutboxException(String message, @Nullable Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates an OutboxException with a cause.
     *
     * @param cause the underlying cause
     */
    public OutboxException(Throwable cause) {
        super(cause);
    }
}
