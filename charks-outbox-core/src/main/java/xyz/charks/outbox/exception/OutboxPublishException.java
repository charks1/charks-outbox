package xyz.charks.outbox.exception;

import xyz.charks.outbox.core.OutboxEventId;
import org.jspecify.annotations.Nullable;

/**
 * Exception thrown when publishing an outbox event to the message broker fails.
 *
 * <p>This exception is thrown by {@link xyz.charks.outbox.broker.BrokerConnector}
 * implementations when message delivery fails. Common causes include:
 * <ul>
 *   <li>Broker connection failures</li>
 *   <li>Authentication/authorization errors</li>
 *   <li>Topic not found</li>
 *   <li>Message size limits exceeded</li>
 *   <li>Timeout during send</li>
 * </ul>
 *
 * <p>The exception optionally includes the event ID for correlation with logs.
 */
public class OutboxPublishException extends OutboxException {

    private final @Nullable OutboxEventId eventId;

    /**
     * Creates an OutboxPublishException with a message.
     *
     * @param message the error message
     */
    public OutboxPublishException(String message) {
        super(message);
        this.eventId = null;
    }

    /**
     * Creates an OutboxPublishException with a message and cause.
     *
     * @param message the error message
     * @param cause the underlying broker exception
     */
    public OutboxPublishException(String message, @Nullable Throwable cause) {
        super(message, cause);
        this.eventId = null;
    }

    /**
     * Creates an OutboxPublishException with event ID, message, and cause.
     *
     * @param eventId the ID of the event that failed to publish
     * @param message the error message
     * @param cause the underlying broker exception
     */
    public OutboxPublishException(OutboxEventId eventId, String message, @Nullable Throwable cause) {
        super(message, cause);
        this.eventId = eventId;
    }

    /**
     * Creates an OutboxPublishException with event ID and message.
     *
     * @param eventId the ID of the event that failed to publish
     * @param message the error message
     */
    public OutboxPublishException(OutboxEventId eventId, String message) {
        super(message);
        this.eventId = eventId;
    }

    /**
     * Returns the ID of the event that failed to publish.
     *
     * @return the event ID, or null if not available
     */
    public @Nullable OutboxEventId getEventId() {
        return eventId;
    }
}
