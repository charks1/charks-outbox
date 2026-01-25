package xyz.charks.outbox.test;

import xyz.charks.outbox.broker.BrokerConnector;
import xyz.charks.outbox.broker.PublishResult;
import xyz.charks.outbox.core.OutboxEvent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Mock implementation of {@link BrokerConnector} for testing.
 *
 * <p>Captures all published events for verification and allows
 * configuring success/failure behavior for testing error scenarios.
 *
 * <p>Features:
 * <ul>
 *   <li>Records all publish attempts</li>
 *   <li>Configurable failure behavior</li>
 *   <li>Health check simulation</li>
 *   <li>Partition and offset tracking</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * MockBrokerConnector connector = new MockBrokerConnector();
 *
 * // Test successful publish
 * connector.publish(event);
 * assertThat(connector.publishedEvents()).hasSize(1);
 *
 * // Test failure handling
 * connector.failNextPublish("Simulated failure");
 * PublishResult result = connector.publish(event);
 * assertThat(result.isFailure()).isTrue();
 *
 * // Reset between tests
 * connector.reset();
 * }</pre>
 *
 * @see BrokerConnector
 */
public class MockBrokerConnector implements BrokerConnector {

    private final List<OutboxEvent> publishedEvents = new CopyOnWriteArrayList<>();
    private final AtomicInteger partitionCounter = new AtomicInteger(0);
    private final AtomicInteger offsetCounter = new AtomicInteger(0);
    private final AtomicBoolean healthy = new AtomicBoolean(true);

    private volatile Function<OutboxEvent, PublishResult> publishBehavior;
    private volatile String nextFailureMessage;
    private volatile int failCount;

    /**
     * Creates a new mock connector with default success behavior.
     */
    public MockBrokerConnector() {
        this.publishBehavior = this::defaultPublish;
    }

    @Override
    public PublishResult publish(OutboxEvent event) {
        Objects.requireNonNull(event, "Event cannot be null");

        publishedEvents.add(event);

        if (nextFailureMessage != null && failCount > 0) {
            failCount--;
            String message = nextFailureMessage;
            if (failCount == 0) {
                nextFailureMessage = null;
            }
            return PublishResult.failure(event.id(), message);
        }

        return publishBehavior.apply(event);
    }

    @Override
    public boolean isHealthy() {
        return healthy.get();
    }

    @Override
    public void close() {
        // No-op for mock
    }

    /**
     * Returns all events that were published.
     *
     * @return unmodifiable list of published events
     */
    public List<OutboxEvent> publishedEvents() {
        return Collections.unmodifiableList(new ArrayList<>(publishedEvents));
    }

    /**
     * Returns the count of publish attempts.
     *
     * @return number of publish calls
     */
    public int publishCount() {
        return publishedEvents.size();
    }

    /**
     * Returns the last published event.
     *
     * @return the last event, or null if none published
     */
    public OutboxEvent lastPublished() {
        if (publishedEvents.isEmpty()) {
            return null;
        }
        return publishedEvents.get(publishedEvents.size() - 1);
    }

    /**
     * Configures the next publish to fail with the given message.
     *
     * @param message the error message
     */
    public void failNextPublish(String message) {
        failNextPublishes(1, message);
    }

    /**
     * Configures the next N publishes to fail.
     *
     * @param count number of publishes to fail
     * @param message the error message
     */
    public void failNextPublishes(int count, String message) {
        this.failCount = count;
        this.nextFailureMessage = message;
    }

    /**
     * Configures all future publishes to fail.
     *
     * @param message the error message
     */
    public void failAllPublishes(String message) {
        this.publishBehavior = event -> PublishResult.failure(event.id(), message);
    }

    /**
     * Configures custom publish behavior.
     *
     * @param behavior function that produces a PublishResult for each event
     */
    public void setPublishBehavior(Function<OutboxEvent, PublishResult> behavior) {
        this.publishBehavior = Objects.requireNonNull(behavior, "Behavior cannot be null");
    }

    /**
     * Resets to default success behavior.
     */
    public void succeedAllPublishes() {
        this.publishBehavior = this::defaultPublish;
        this.nextFailureMessage = null;
        this.failCount = 0;
    }

    /**
     * Sets the health status returned by {@link #isHealthy()}.
     *
     * @param healthy true for healthy, false for unhealthy
     */
    public void setHealthy(boolean healthy) {
        this.healthy.set(healthy);
    }

    /**
     * Clears all recorded events and resets state.
     *
     * <p>Call this method between tests to ensure isolation.
     */
    public void reset() {
        publishedEvents.clear();
        partitionCounter.set(0);
        offsetCounter.set(0);
        healthy.set(true);
        publishBehavior = this::defaultPublish;
        nextFailureMessage = null;
        failCount = 0;
    }

    /**
     * Checks if a specific event was published.
     *
     * @param event the event to check
     * @return true if the event was published
     */
    public boolean wasPublished(OutboxEvent event) {
        return publishedEvents.contains(event);
    }

    /**
     * Returns events published to a specific topic.
     *
     * @param topic the topic name
     * @return events published to that topic
     */
    public List<OutboxEvent> eventsForTopic(String topic) {
        return publishedEvents.stream()
                .filter(e -> e.topic().equals(topic))
                .toList();
    }

    private PublishResult defaultPublish(OutboxEvent event) {
        int partition = Math.abs(
                event.partitionKey() != null ? event.partitionKey().hashCode() : 0
        ) % 4;
        long offset = offsetCounter.incrementAndGet();

        return PublishResult.success(
                event.id(),
                "mock-" + event.id().value(),
                partition,
                offset
        );
    }
}
