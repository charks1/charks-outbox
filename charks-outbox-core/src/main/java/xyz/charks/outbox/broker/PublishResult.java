package xyz.charks.outbox.broker;

import xyz.charks.outbox.core.OutboxEventId;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * Result of publishing an outbox event to the message broker.
 *
 * <p>Contains broker-specific metadata about the published message, such as
 * offset, partition, or message ID depending on the broker type.
 *
 * @param eventId the ID of the published event
 * @param success true if publishing succeeded
 * @param publishedAt timestamp of successful publication (null if failed)
 * @param brokerMessageId broker-assigned message identifier (null if unavailable or failed)
 * @param partition target partition or queue (null if not applicable)
 * @param offset message offset within the partition (null if not applicable)
 * @param error error message if publishing failed (null if successful)
 */
public record PublishResult(
        OutboxEventId eventId,
        boolean success,
        @Nullable Instant publishedAt,
        @Nullable String brokerMessageId,
        @Nullable Integer partition,
        @Nullable Long offset,
        @Nullable String error
) {

    /**
     * Creates a PublishResult with validation.
     *
     * @throws NullPointerException if eventId is null
     */
    public PublishResult {
        Objects.requireNonNull(eventId, "Event ID cannot be null");
    }

    /**
     * Creates a successful publish result.
     *
     * @param eventId the ID of the published event
     * @return a successful result with current timestamp
     */
    public static PublishResult success(OutboxEventId eventId) {
        return new PublishResult(eventId, true, Instant.now(), null, null, null, null);
    }

    /**
     * Creates a successful publish result with broker metadata.
     *
     * @param eventId the ID of the published event
     * @param brokerMessageId broker-assigned message ID
     * @param partition target partition
     * @param offset message offset
     * @return a successful result with metadata
     */
    public static PublishResult success(
            OutboxEventId eventId,
            @Nullable String brokerMessageId,
            @Nullable Integer partition,
            @Nullable Long offset
    ) {
        return new PublishResult(eventId, true, Instant.now(), brokerMessageId, partition, offset, null);
    }

    /**
     * Creates a failed publish result.
     *
     * @param eventId the ID of the event that failed to publish
     * @param error description of the failure
     * @return a failure result
     */
    public static PublishResult failure(OutboxEventId eventId, String error) {
        Objects.requireNonNull(error, "Error message cannot be null for failure");
        return new PublishResult(eventId, false, null, null, null, null, error);
    }

    /**
     * Creates a failed publish result from an exception.
     *
     * @param eventId the ID of the event that failed to publish
     * @param cause the exception that caused the failure
     * @return a failure result with exception message
     */
    public static PublishResult failure(OutboxEventId eventId, Throwable cause) {
        Objects.requireNonNull(cause, "Cause cannot be null");
        String errorMessage = cause.getMessage() != null ? cause.getMessage() : cause.getClass().getName();
        return new PublishResult(eventId, false, null, null, null, null, errorMessage);
    }

    /**
     * Returns the published timestamp if available.
     *
     * @return optional containing the publish timestamp, empty if failed
     */
    public Optional<Instant> getPublishedAt() {
        return Optional.ofNullable(publishedAt);
    }

    /**
     * Returns the broker message ID if available.
     *
     * @return optional containing the broker message ID
     */
    public Optional<String> getBrokerMessageId() {
        return Optional.ofNullable(brokerMessageId);
    }

    /**
     * Returns the partition if available.
     *
     * @return optional containing the partition number
     */
    public Optional<Integer> getPartition() {
        return Optional.ofNullable(partition);
    }

    /**
     * Returns the offset if available.
     *
     * @return optional containing the offset
     */
    public Optional<Long> getOffset() {
        return Optional.ofNullable(offset);
    }

    /**
     * Returns the error message if publishing failed.
     *
     * @return optional containing the error message
     */
    public Optional<String> getError() {
        return Optional.ofNullable(error);
    }

    /**
     * Checks if this result represents a failure.
     *
     * @return true if publishing failed
     */
    public boolean isFailure() {
        return !success;
    }
}
