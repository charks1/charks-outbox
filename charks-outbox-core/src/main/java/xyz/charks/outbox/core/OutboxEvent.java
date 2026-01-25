package xyz.charks.outbox.core;

import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable representation of an outbox event.
 *
 * <p>An outbox event captures the data needed to reliably publish a domain event
 * to a message broker. Events are stored in a database table within the same
 * transaction as the business operation, ensuring atomicity.
 *
 * <p>Use {@link OutboxEventBuilder} for constructing instances:
 * <pre>{@code
 * OutboxEvent event = OutboxEvent.builder()
 *     .aggregateType("Order")
 *     .aggregateId(AggregateId.of(orderId))
 *     .eventType(EventType.of("OrderCreated"))
 *     .topic("orders")
 *     .payload(serializedPayload)
 *     .build();
 * }</pre>
 *
 * @param id unique identifier for this event
 * @param aggregateType the type of aggregate that produced this event
 * @param aggregateId identifier of the source aggregate
 * @param eventType discriminator for event processing
 * @param topic destination topic or queue name
 * @param partitionKey optional key for ordered delivery (nullable)
 * @param payload serialized event data
 * @param headers optional metadata headers
 * @param createdAt timestamp when the event was created
 * @param status current lifecycle status
 * @param retryCount number of publish attempts
 * @param lastError description of the last failure (nullable)
 * @param processedAt timestamp of successful publishing (nullable)
 *
 * @see OutboxEventBuilder
 * @see OutboxStatus
 */
public record OutboxEvent(
        OutboxEventId id,
        String aggregateType,
        AggregateId aggregateId,
        EventType eventType,
        String topic,
        @Nullable String partitionKey,
        byte[] payload,
        Map<String, String> headers,
        Instant createdAt,
        OutboxStatus status,
        int retryCount,
        @Nullable String lastError,
        @Nullable Instant processedAt
) {

    /**
     * Creates an OutboxEvent with validation.
     *
     * @throws NullPointerException if any required field is null
     * @throws IllegalArgumentException if aggregateType or topic is blank
     */
    public OutboxEvent {
        Objects.requireNonNull(id, "Event ID cannot be null");
        Objects.requireNonNull(aggregateType, "Aggregate type cannot be null");
        Objects.requireNonNull(aggregateId, "Aggregate ID cannot be null");
        Objects.requireNonNull(eventType, "Event type cannot be null");
        Objects.requireNonNull(topic, "Topic cannot be null");
        Objects.requireNonNull(payload, "Payload cannot be null");
        Objects.requireNonNull(headers, "Headers cannot be null");
        Objects.requireNonNull(createdAt, "Created at cannot be null");
        Objects.requireNonNull(status, "Status cannot be null");

        if (aggregateType.isBlank()) {
            throw new IllegalArgumentException("Aggregate type cannot be blank");
        }
        if (topic.isBlank()) {
            throw new IllegalArgumentException("Topic cannot be blank");
        }
        if (retryCount < 0) {
            throw new IllegalArgumentException("Retry count cannot be negative");
        }

        payload = payload.clone();
        headers = Map.copyOf(headers);
    }

    /**
     * Returns a defensive copy of the payload.
     *
     * @return copy of the payload bytes
     */
    @Override
    public byte[] payload() {
        return payload.clone();
    }

    /**
     * Returns an unmodifiable view of the headers.
     *
     * @return immutable header map
     */
    @Override
    public Map<String, String> headers() {
        return Collections.unmodifiableMap(headers);
    }

    /**
     * Creates a new builder for constructing OutboxEvent instances.
     *
     * @return a new OutboxEventBuilder
     */
    public static OutboxEventBuilder builder() {
        return new OutboxEventBuilder();
    }

    /**
     * Creates a builder initialized with values from this event.
     *
     * <p>Useful for creating modified copies of an event.
     *
     * @return a new OutboxEventBuilder pre-populated with this event's values
     */
    public OutboxEventBuilder toBuilder() {
        return new OutboxEventBuilder()
                .id(this.id)
                .aggregateType(this.aggregateType)
                .aggregateId(this.aggregateId)
                .eventType(this.eventType)
                .topic(this.topic)
                .partitionKey(this.partitionKey)
                .payload(this.payload)
                .headers(this.headers)
                .createdAt(this.createdAt)
                .status(this.status)
                .retryCount(this.retryCount)
                .lastError(this.lastError)
                .processedAt(this.processedAt);
    }

    /**
     * Returns a copy with the status updated.
     *
     * @param newStatus the new status
     * @return a new OutboxEvent with the updated status
     */
    public OutboxEvent withStatus(OutboxStatus newStatus) {
        return toBuilder().status(newStatus).build();
    }

    /**
     * Returns a copy marked as published.
     *
     * @param publishedAt the time of successful publication
     * @return a new OutboxEvent with Published status
     */
    public OutboxEvent markPublished(Instant publishedAt) {
        return toBuilder()
                .status(new Published(publishedAt))
                .processedAt(publishedAt)
                .build();
    }

    /**
     * Returns a copy marked as failed.
     *
     * @param error description of the failure
     * @return a new OutboxEvent with incremented retry count and Failed status
     */
    public OutboxEvent markFailed(String error) {
        return toBuilder()
                .status(new Failed(error, this.retryCount + 1, Instant.now()))
                .retryCount(this.retryCount + 1)
                .lastError(error)
                .build();
    }

    /**
     * Checks if this event is in pending status.
     *
     * @return true if the event is pending
     */
    public boolean isPending() {
        return status instanceof Pending;
    }

    /**
     * Checks if this event has been published.
     *
     * @return true if the event has been published
     */
    public boolean isPublished() {
        return status instanceof Published;
    }

    /**
     * Checks if this event has failed.
     *
     * @return true if the event has failed
     */
    public boolean isFailed() {
        return status instanceof Failed;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof OutboxEvent that)) return false;
        return retryCount == that.retryCount
                && Objects.equals(id, that.id)
                && Objects.equals(aggregateType, that.aggregateType)
                && Objects.equals(aggregateId, that.aggregateId)
                && Objects.equals(eventType, that.eventType)
                && Objects.equals(topic, that.topic)
                && Objects.equals(partitionKey, that.partitionKey)
                && Arrays.equals(payload, that.payload)
                && Objects.equals(headers, that.headers)
                && Objects.equals(createdAt, that.createdAt)
                && Objects.equals(status, that.status)
                && Objects.equals(lastError, that.lastError)
                && Objects.equals(processedAt, that.processedAt);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(id, aggregateType, aggregateId, eventType, topic,
                partitionKey, headers, createdAt, status, retryCount, lastError, processedAt);
        result = 31 * result + Arrays.hashCode(payload);
        return result;
    }
}
