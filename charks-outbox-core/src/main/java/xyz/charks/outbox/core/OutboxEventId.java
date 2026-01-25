package xyz.charks.outbox.core;

import java.util.Objects;
import java.util.UUID;

/**
 * Type-safe wrapper for outbox event identifiers.
 *
 * <p>Provides compile-time type safety to prevent accidental mixing of different
 * identifier types in method signatures.
 *
 * @param value the underlying UUID value (never null)
 */
public record OutboxEventId(UUID value) {

    /**
     * Creates an OutboxEventId with the given UUID.
     *
     * @param value the UUID value
     * @throws NullPointerException if value is null
     */
    public OutboxEventId {
        Objects.requireNonNull(value, "Event ID cannot be null");
    }

    /**
     * Creates a new OutboxEventId with a random UUID.
     *
     * @return a new OutboxEventId with a randomly generated UUID
     */
    public static OutboxEventId generate() {
        return new OutboxEventId(UUID.randomUUID());
    }

    /**
     * Creates an OutboxEventId from a string representation.
     *
     * @param id the string representation of a UUID
     * @return a new OutboxEventId
     * @throws IllegalArgumentException if the string is not a valid UUID
     * @throws NullPointerException if id is null
     */
    public static OutboxEventId fromString(String id) {
        Objects.requireNonNull(id, "ID string cannot be null");
        return new OutboxEventId(UUID.fromString(id));
    }

    @Override
    public String toString() {
        return value.toString();
    }
}
