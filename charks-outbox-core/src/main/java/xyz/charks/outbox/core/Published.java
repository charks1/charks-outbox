package xyz.charks.outbox.core;

import java.time.Instant;
import java.util.Objects;

/**
 * Represents an event that has been successfully published to the message broker.
 *
 * <p>This is a terminal state. Published events can be cleaned up by an archival
 * process or deleted directly, depending on retention policy.
 *
 * @param publishedAt timestamp when the event was successfully published
 */
public record Published(Instant publishedAt) implements OutboxStatus {

    /**
     * Creates a Published status with the given publish time.
     *
     * @param publishedAt timestamp when the event was published
     * @throws NullPointerException if publishedAt is null
     */
    public Published {
        Objects.requireNonNull(publishedAt, "Published at cannot be null");
    }

    /**
     * Creates a new Published status with the current timestamp.
     *
     * @return a new Published instance
     */
    public static Published now() {
        return new Published(Instant.now());
    }

    /**
     * Creates a Published status with a specific timestamp.
     *
     * @param publishedAt the publish timestamp
     * @return a new Published instance
     */
    public static Published at(Instant publishedAt) {
        return new Published(publishedAt);
    }

    @Override
    public String name() {
        return "PUBLISHED";
    }

    @Override
    public String toString() {
        return "Published[publishedAt=" + publishedAt + "]";
    }
}
