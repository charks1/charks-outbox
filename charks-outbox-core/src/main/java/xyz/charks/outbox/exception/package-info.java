/**
 * Exception hierarchy for outbox operations.
 *
 * <p>All outbox-specific exceptions extend {@link xyz.charks.outbox.exception.OutboxException},
 * providing a consistent error handling experience:
 * <ul>
 *   <li>{@link xyz.charks.outbox.exception.OutboxPersistenceException} - Database operation failures</li>
 *   <li>{@link xyz.charks.outbox.exception.OutboxPublishException} - Broker publishing failures</li>
 *   <li>{@link xyz.charks.outbox.exception.OutboxSerializationException} - Serialization/deserialization errors</li>
 * </ul>
 *
 * @see xyz.charks.outbox.exception.OutboxException
 */
@NullMarked
package xyz.charks.outbox.exception;

import org.jspecify.annotations.NullMarked;
