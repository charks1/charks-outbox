package xyz.charks.outbox.core;

import java.util.Objects;

/**
 * Represents the identifier of a domain aggregate that produced an event.
 *
 * <p>The aggregate ID is used for:
 * <ul>
 *   <li>Event correlation and tracing</li>
 *   <li>Partition key calculation for ordered delivery</li>
 *   <li>Filtering events by source aggregate</li>
 * </ul>
 *
 * <p>The value is stored as a string to support various ID formats (UUID, numeric, composite).
 *
 * @param value the aggregate identifier value (never null, may be empty)
 */
public record AggregateId(String value) {

    /**
     * Creates an AggregateId with the given string value.
     *
     * @param value the aggregate identifier
     * @throws NullPointerException if value is null
     */
    public AggregateId {
        Objects.requireNonNull(value, "Aggregate ID cannot be null");
    }

    /**
     * Creates an AggregateId from an object's string representation.
     *
     * @param id the object whose string representation forms the ID
     * @return a new AggregateId
     * @throws NullPointerException if id is null
     */
    public static AggregateId of(Object id) {
        Objects.requireNonNull(id, "ID cannot be null");
        return new AggregateId(id.toString());
    }

    /**
     * Returns an empty aggregate ID.
     *
     * <p>Useful for events that are not associated with a specific aggregate.
     *
     * @return an AggregateId with an empty string value
     */
    public static AggregateId empty() {
        return new AggregateId("");
    }

    /**
     * Checks if this aggregate ID is empty.
     *
     * @return true if the value is empty, false otherwise
     */
    public boolean isEmpty() {
        return value.isEmpty();
    }

    @Override
    public String toString() {
        return value;
    }
}
