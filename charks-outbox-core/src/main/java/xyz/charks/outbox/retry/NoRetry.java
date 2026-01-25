package xyz.charks.outbox.retry;

import xyz.charks.outbox.core.OutboxEvent;

import java.time.Duration;
import java.util.Optional;

/**
 * Retry policy that never retries failed events.
 *
 * <p>Each event gets exactly one attempt. Use when:
 * <ul>
 *   <li>The broker guarantees at-least-once delivery after acknowledgment</li>
 *   <li>Retries are handled at a different layer</li>
 *   <li>Failed events should immediately go to dead letter</li>
 * </ul>
 *
 * <p>Example:
 * <pre>{@code
 * RetryPolicy policy = NoRetry.instance();
 * }</pre>
 */
public final class NoRetry implements RetryPolicy {

    private static final NoRetry INSTANCE = new NoRetry();

    private NoRetry() {}

    /**
     * Returns the singleton NoRetry instance.
     *
     * @return the NoRetry instance
     */
    public static NoRetry instance() {
        return INSTANCE;
    }

    @Override
    public Optional<Duration> nextRetryDelay(OutboxEvent event, Throwable error) {
        return Optional.empty();
    }

    @Override
    public int maxAttempts() {
        return 1;
    }

    @Override
    public boolean shouldRetry(OutboxEvent event) {
        return false;
    }
}
