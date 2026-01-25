package xyz.charks.outbox.retry;

import xyz.charks.outbox.core.OutboxEvent;

import java.time.Duration;
import java.util.Optional;

/**
 * Strategy interface for determining retry behavior for failed outbox events.
 *
 * <p>Implementations define:
 * <ul>
 *   <li>Maximum number of retry attempts</li>
 *   <li>Delay between attempts</li>
 *   <li>Whether specific failures are retryable</li>
 * </ul>
 *
 * <p>The relay uses this policy to decide when to retry failed events
 * and how long to wait between attempts.
 *
 * <p>Built-in implementations:
 * <ul>
 *   <li>{@link ExponentialBackoff} - Increasing delays with optional jitter</li>
 *   <li>{@link FixedDelay} - Constant delay between attempts</li>
 *   <li>{@link NoRetry} - Single attempt only</li>
 * </ul>
 *
 * @see ExponentialBackoff
 * @see FixedDelay
 * @see NoRetry
 */
public interface RetryPolicy {

    /**
     * Determines if the event should be retried and the delay before the next attempt.
     *
     * <p>Returns empty if the event should not be retried (either because it has
     * exhausted its attempts or the error is not retryable).
     *
     * @param event the event that failed to publish
     * @param error the error that caused the failure
     * @return optional containing the delay before retry, empty if no retry
     */
    Optional<Duration> nextRetryDelay(OutboxEvent event, Throwable error);

    /**
     * Returns the maximum number of retry attempts.
     *
     * <p>After this many attempts (including the initial attempt), the event
     * will not be retried further.
     *
     * @return the maximum number of attempts
     */
    int maxAttempts();

    /**
     * Checks if the event should be retried based on the current attempt count.
     *
     * <p>Default implementation compares retry count against max attempts.
     *
     * @param event the event to check
     * @return true if the event can be retried
     */
    default boolean shouldRetry(OutboxEvent event) {
        return event.retryCount() < maxAttempts();
    }

    /**
     * Checks if the given error is retryable.
     *
     * <p>Override this to skip retries for certain error types that cannot succeed.
     *
     * @param error the error that occurred
     * @return true if the error is potentially recoverable
     */
    default boolean isRetryable(Throwable error) {
        return true;
    }
}
