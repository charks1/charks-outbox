package xyz.charks.outbox.core;

import java.util.Objects;

/**
 * Represents the type of a domain event.
 *
 * <p>Event types serve as discriminators for consumers to determine how to process
 * incoming events. Common naming conventions include:
 * <ul>
 *   <li>Dot notation: {@code com.example.OrderCreated}</li>
 *   <li>Simple name: {@code OrderCreated}</li>
 *   <li>Versioned: {@code order.created.v1}</li>
 * </ul>
 *
 * @param value the event type identifier (never null or blank)
 */
public record EventType(String value) {

    /**
     * Creates an EventType with the given value.
     *
     * @param value the event type identifier
     * @throws NullPointerException if value is null
     * @throws IllegalArgumentException if value is blank
     */
    public EventType {
        Objects.requireNonNull(value, "Event type cannot be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException("Event type cannot be blank");
        }
    }

    /**
     * Creates an EventType from a class, using its fully qualified name.
     *
     * @param eventClass the class representing the event type
     * @return a new EventType with the class name as value
     * @throws NullPointerException if eventClass is null
     */
    public static EventType fromClass(Class<?> eventClass) {
        Objects.requireNonNull(eventClass, "Event class cannot be null");
        return new EventType(eventClass.getName());
    }

    /**
     * Creates an EventType from a class, using only its simple name.
     *
     * @param eventClass the class representing the event type
     * @return a new EventType with the simple class name as value
     * @throws NullPointerException if eventClass is null
     */
    public static EventType fromSimpleName(Class<?> eventClass) {
        Objects.requireNonNull(eventClass, "Event class cannot be null");
        return new EventType(eventClass.getSimpleName());
    }

    /**
     * Creates an EventType from a string value.
     *
     * @param type the event type string
     * @return a new EventType
     * @throws NullPointerException if type is null
     * @throws IllegalArgumentException if type is blank
     */
    public static EventType of(String type) {
        return new EventType(type);
    }

    @Override
    public String toString() {
        return value;
    }
}
