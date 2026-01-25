package xyz.charks.outbox.core;

import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.Objects;

/**
 * Represents an event that has been archived.
 *
 * <p>Archived events are no longer processed. This state is used for:
 * <ul>
 *   <li>Successfully published events awaiting cleanup</li>
 *   <li>Failed events moved to dead letter after exhausting retries</li>
 *   <li>Events manually archived by administrators</li>
 * </ul>
 *
 * @param archivedAt timestamp when the event was archived
 * @param reason optional description of why the event was archived
 */
public record Archived(Instant archivedAt, @Nullable String reason) implements OutboxStatus {

    /**
     * Creates an Archived status with the given parameters.
     *
     * @param archivedAt timestamp when the event was archived
     * @param reason optional description of the archive reason
     * @throws NullPointerException if archivedAt is null
     */
    public Archived {
        Objects.requireNonNull(archivedAt, "Archived at cannot be null");
    }

    /**
     * Creates an Archived status with current timestamp and no reason.
     *
     * @return a new Archived instance
     */
    public static Archived now() {
        return new Archived(Instant.now(), null);
    }

    /**
     * Creates an Archived status with current timestamp and a reason.
     *
     * @param reason description of why the event was archived
     * @return a new Archived instance
     */
    public static Archived withReason(String reason) {
        return new Archived(Instant.now(), reason);
    }

    /**
     * Creates an Archived status at a specific time.
     *
     * @param archivedAt the archive timestamp
     * @return a new Archived instance
     */
    public static Archived at(Instant archivedAt) {
        return new Archived(archivedAt, null);
    }

    @Override
    public String name() {
        return "ARCHIVED";
    }

    @Override
    public String toString() {
        return reason != null
                ? "Archived[archivedAt=" + archivedAt + ", reason=" + reason + "]"
                : "Archived[archivedAt=" + archivedAt + "]";
    }
}
