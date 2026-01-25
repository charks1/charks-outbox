package xyz.charks.outbox.core;

import java.time.Instant;
import java.util.Objects;

/**
 * Represents an event that failed to publish.
 *
 * <p>Failed events may be retried according to the configured retry policy.
 * After exhausting retries, events can be moved to a dead letter queue or
 * archived for manual investigation.
 *
 * @param error description of the failure cause
 * @param attemptCount number of publish attempts made
 * @param failedAt timestamp of the last failure
 */
public record Failed(String error, int attemptCount, Instant failedAt) implements OutboxStatus {

    /**
     * Creates a Failed status with the given parameters.
     *
     * @param error description of the failure
     * @param attemptCount number of attempts made
     * @param failedAt timestamp of the failure
     * @throws NullPointerException if error or failedAt is null
     * @throws IllegalArgumentException if attemptCount is negative
     */
    public Failed {
        Objects.requireNonNull(error, "Error cannot be null");
        Objects.requireNonNull(failedAt, "Failed at cannot be null");
        if (attemptCount < 0) {
            throw new IllegalArgumentException("Attempt count cannot be negative");
        }
    }

    /**
     * Creates a Failed status with current timestamp.
     *
     * @param error description of the failure
     * @param attemptCount number of attempts made
     * @return a new Failed instance
     */
    public static Failed of(String error, int attemptCount) {
        return new Failed(error, attemptCount, Instant.now());
    }

    /**
     * Creates a Failed status for the first failure.
     *
     * @param error description of the failure
     * @return a new Failed instance with attempt count of 1
     */
    public static Failed firstAttempt(String error) {
        return new Failed(error, 1, Instant.now());
    }

    @Override
    public String name() {
        return "FAILED";
    }

    @Override
    public String toString() {
        return "Failed[error=" + error + ", attemptCount=" + attemptCount + ", failedAt=" + failedAt + "]";
    }
}
