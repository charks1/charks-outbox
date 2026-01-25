package xyz.charks.outbox.core;

import java.time.Instant;
import java.util.Objects;

/**
 * Represents an event that is waiting to be published.
 *
 * <p>This is the initial state for all outbox events. Events remain in this state
 * until they are successfully published or exhaust their retry attempts.
 *
 * @param enqueuedAt timestamp when the event entered pending state
 */
public record Pending(Instant enqueuedAt) implements OutboxStatus {

    /**
     * Creates a Pending status with the given enqueue time.
     *
     * @param enqueuedAt timestamp when the event entered pending state
     * @throws NullPointerException if enqueuedAt is null
     */
    public Pending {
        Objects.requireNonNull(enqueuedAt, "Enqueued at cannot be null");
    }

    /**
     * Creates a new Pending status with the current timestamp.
     *
     * @return a new Pending instance
     */
    public static Pending create() {
        return new Pending(Instant.now());
    }

    /**
     * Creates a Pending status with a specific timestamp.
     *
     * @param enqueuedAt the enqueue timestamp
     * @return a new Pending instance
     */
    public static Pending at(Instant enqueuedAt) {
        return new Pending(enqueuedAt);
    }

    @Override
    public String name() {
        return "PENDING";
    }

    @Override
    public String toString() {
        return "Pending[enqueuedAt=" + enqueuedAt + "]";
    }
}
