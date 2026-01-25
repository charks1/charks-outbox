package xyz.charks.outbox.exception;

import org.jspecify.annotations.Nullable;

/**
 * Exception thrown when outbox persistence operations fail.
 *
 * <p>This exception wraps database-related errors such as:
 * <ul>
 *   <li>Connection failures</li>
 *   <li>Constraint violations</li>
 *   <li>Query execution errors</li>
 *   <li>Transaction failures</li>
 * </ul>
 *
 * <p>Example handling:
 * <pre>{@code
 * try {
 *     repository.save(event);
 * } catch (OutboxPersistenceException e) {
 *     logger.error("Failed to persist outbox event: {}", e.getMessage());
 *     throw new BusinessException("Order creation failed", e);
 * }
 * }</pre>
 */
public class OutboxPersistenceException extends OutboxException {

    /**
     * Creates an OutboxPersistenceException with a message.
     *
     * @param message the error message
     */
    public OutboxPersistenceException(String message) {
        super(message);
    }

    /**
     * Creates an OutboxPersistenceException with a message and cause.
     *
     * @param message the error message
     * @param cause the underlying database exception
     */
    public OutboxPersistenceException(String message, @Nullable Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates an OutboxPersistenceException with a cause.
     *
     * @param cause the underlying database exception
     */
    public OutboxPersistenceException(Throwable cause) {
        super(cause);
    }
}
