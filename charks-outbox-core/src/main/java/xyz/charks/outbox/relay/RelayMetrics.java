package xyz.charks.outbox.relay;

import xyz.charks.outbox.core.OutboxEvent;
import xyz.charks.outbox.core.OutboxEventId;

import java.time.Duration;

/**
 * Callback interface for relay metrics collection.
 *
 * <p>Implement this interface to integrate with your monitoring system
 * (Micrometer, OpenTelemetry, etc.). The relay calls these methods
 * at key points during event processing.
 *
 * <p>Example Micrometer implementation:
 * <pre>{@code
 * public class MicrometerRelayMetrics implements RelayMetrics {
 *     private final Counter publishedCounter;
 *     private final Counter failedCounter;
 *     private final Timer batchTimer;
 *
 *     public void onEventPublished(OutboxEvent event, Duration duration) {
 *         publishedCounter.increment();
 *     }
 *
 *     public void onEventFailed(OutboxEvent event, Throwable error) {
 *         failedCounter.increment();
 *     }
 *
 *     public void onBatchCompleted(int processed, int failed, Duration duration) {
 *         batchTimer.record(duration);
 *     }
 * }
 * }</pre>
 */
public interface RelayMetrics {

    /**
     * Called when an event is successfully published.
     *
     * @param event the published event
     * @param duration time taken to publish
     */
    default void onEventPublished(OutboxEvent event, Duration duration) {}

    /**
     * Called when an event fails to publish.
     *
     * @param event the failed event
     * @param error the cause of the failure
     */
    default void onEventFailed(OutboxEvent event, Throwable error) {}

    /**
     * Called when an event is retried.
     *
     * @param eventId the event ID being retried
     * @param attemptNumber the current attempt number
     * @param delayUntilRetry time until the next retry
     */
    default void onEventRetryScheduled(OutboxEventId eventId, int attemptNumber, Duration delayUntilRetry) {}

    /**
     * Called when an event exhausts all retry attempts.
     *
     * @param event the event that will not be retried
     * @param totalAttempts total attempts made
     * @param lastError the last error encountered
     */
    default void onEventExhausted(OutboxEvent event, int totalAttempts, Throwable lastError) {}

    /**
     * Called when a batch processing cycle completes.
     *
     * @param processed total events processed in the batch
     * @param succeeded events successfully published
     * @param failed events that failed to publish
     * @param duration total time for the batch
     */
    default void onBatchCompleted(int processed, int succeeded, int failed, Duration duration) {}

    /**
     * Called at the start of each polling cycle.
     */
    default void onPollingCycleStart() {}

    /**
     * Called at the end of each polling cycle.
     *
     * @param eventsFetched number of events fetched from the database
     * @param fetchDuration time taken to fetch events
     */
    default void onPollingCycleEnd(int eventsFetched, Duration fetchDuration) {}

    /**
     * Called when the relay starts.
     */
    default void onRelayStarted() {}

    /**
     * Called when the relay stops.
     *
     * @param graceful true if shutdown was graceful
     */
    default void onRelayStopped(boolean graceful) {}

    /**
     * Returns a no-op metrics instance.
     *
     * @return a metrics instance that does nothing
     */
    static RelayMetrics noop() {
        return NoopRelayMetrics.INSTANCE;
    }
}

/**
 * No-operation implementation for when metrics are not needed.
 */
enum NoopRelayMetrics implements RelayMetrics {
    INSTANCE
}
