package xyz.charks.outbox.core;

import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Builder for constructing {@link OutboxEvent} instances.
 *
 * <p>Provides a fluent API for creating outbox events with sensible defaults:
 * <ul>
 *   <li>ID is auto-generated if not specified</li>
 *   <li>Creation timestamp defaults to current time</li>
 *   <li>Status defaults to {@link Pending}</li>
 *   <li>Retry count defaults to 0</li>
 *   <li>Headers default to empty map</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * OutboxEvent event = OutboxEvent.builder()
 *     .aggregateType("Order")
 *     .aggregateId(AggregateId.of(orderId))
 *     .eventType(EventType.of("OrderCreated"))
 *     .topic("orders")
 *     .partitionKey(orderId.toString())
 *     .payload(jsonBytes)
 *     .header("correlation-id", correlationId)
 *     .build();
 * }</pre>
 */
public final class OutboxEventBuilder {

    private @Nullable OutboxEventId id;
    private @Nullable String aggregateType;
    private @Nullable AggregateId aggregateId;
    private @Nullable EventType eventType;
    private @Nullable String topic;
    private @Nullable String partitionKey;
    private byte @Nullable [] payload;
    private final Map<String, String> headers = new HashMap<>();
    private @Nullable Instant createdAt;
    private @Nullable OutboxStatus status;
    private int retryCount = 0;
    private @Nullable String lastError;
    private @Nullable Instant processedAt;

    /**
     * Creates a new empty builder.
     */
    OutboxEventBuilder() {
    }

    /**
     * Sets the event ID.
     *
     * @param id the event identifier
     * @return this builder
     */
    public OutboxEventBuilder id(OutboxEventId id) {
        this.id = id;
        return this;
    }

    /**
     * Sets the aggregate type.
     *
     * @param aggregateType the type of aggregate that produced this event
     * @return this builder
     */
    public OutboxEventBuilder aggregateType(String aggregateType) {
        this.aggregateType = aggregateType;
        return this;
    }

    /**
     * Sets the aggregate ID.
     *
     * @param aggregateId identifier of the source aggregate
     * @return this builder
     */
    public OutboxEventBuilder aggregateId(AggregateId aggregateId) {
        this.aggregateId = aggregateId;
        return this;
    }

    /**
     * Sets the aggregate ID from a string value.
     *
     * @param aggregateId identifier of the source aggregate
     * @return this builder
     */
    public OutboxEventBuilder aggregateId(String aggregateId) {
        this.aggregateId = new AggregateId(aggregateId);
        return this;
    }

    /**
     * Sets the event type.
     *
     * @param eventType discriminator for event processing
     * @return this builder
     */
    public OutboxEventBuilder eventType(EventType eventType) {
        this.eventType = eventType;
        return this;
    }

    /**
     * Sets the event type from a string value.
     *
     * @param eventType discriminator for event processing
     * @return this builder
     */
    public OutboxEventBuilder eventType(String eventType) {
        this.eventType = EventType.of(eventType);
        return this;
    }

    /**
     * Sets the destination topic.
     *
     * @param topic destination topic or queue name
     * @return this builder
     */
    public OutboxEventBuilder topic(String topic) {
        this.topic = topic;
        return this;
    }

    /**
     * Sets the partition key for ordered delivery.
     *
     * @param partitionKey key for message partitioning (may be null)
     * @return this builder
     */
    public OutboxEventBuilder partitionKey(@Nullable String partitionKey) {
        this.partitionKey = partitionKey;
        return this;
    }

    /**
     * Sets the event payload.
     *
     * @param payload serialized event data
     * @return this builder
     */
    public OutboxEventBuilder payload(byte[] payload) {
        this.payload = payload != null ? payload.clone() : null;
        return this;
    }

    /**
     * Sets all headers, replacing any existing headers.
     *
     * @param headers header map
     * @return this builder
     */
    public OutboxEventBuilder headers(Map<String, String> headers) {
        this.headers.clear();
        if (headers != null) {
            this.headers.putAll(headers);
        }
        return this;
    }

    /**
     * Adds a single header.
     *
     * @param key header key
     * @param value header value
     * @return this builder
     */
    public OutboxEventBuilder header(String key, String value) {
        this.headers.put(key, value);
        return this;
    }

    /**
     * Sets the creation timestamp.
     *
     * @param createdAt timestamp when the event was created
     * @return this builder
     */
    public OutboxEventBuilder createdAt(Instant createdAt) {
        this.createdAt = createdAt;
        return this;
    }

    /**
     * Sets the event status.
     *
     * @param status lifecycle status
     * @return this builder
     */
    public OutboxEventBuilder status(OutboxStatus status) {
        this.status = status;
        return this;
    }

    /**
     * Sets the retry count.
     *
     * @param retryCount number of publish attempts
     * @return this builder
     */
    public OutboxEventBuilder retryCount(int retryCount) {
        this.retryCount = retryCount;
        return this;
    }

    /**
     * Sets the last error message.
     *
     * @param lastError description of the last failure
     * @return this builder
     */
    public OutboxEventBuilder lastError(@Nullable String lastError) {
        this.lastError = lastError;
        return this;
    }

    /**
     * Sets the processed timestamp.
     *
     * @param processedAt timestamp of successful publishing
     * @return this builder
     */
    public OutboxEventBuilder processedAt(@Nullable Instant processedAt) {
        this.processedAt = processedAt;
        return this;
    }

    /**
     * Builds the OutboxEvent with the configured values.
     *
     * <p>Applies defaults for optional fields:
     * <ul>
     *   <li>ID: generated if null</li>
     *   <li>createdAt: current time if null</li>
     *   <li>status: Pending if null</li>
     * </ul>
     *
     * @return a new OutboxEvent instance
     * @throws NullPointerException if required fields are missing
     */
    public OutboxEvent build() {
        return new OutboxEvent(
                id != null ? id : OutboxEventId.generate(),
                Objects.requireNonNull(aggregateType, "Aggregate type is required"),
                Objects.requireNonNull(aggregateId, "Aggregate ID is required"),
                Objects.requireNonNull(eventType, "Event type is required"),
                Objects.requireNonNull(topic, "Topic is required"),
                partitionKey,
                Objects.requireNonNull(payload, "Payload is required"),
                headers,
                createdAt != null ? createdAt : Instant.now(),
                status != null ? status : Pending.create(),
                retryCount,
                lastError,
                processedAt
        );
    }
}
