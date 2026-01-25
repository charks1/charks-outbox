package xyz.charks.outbox.retry;

import xyz.charks.outbox.core.OutboxEvent;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * Retry policy with a constant delay between attempts.
 *
 * <p>Every retry uses the same delay regardless of attempt number.
 * Simple and predictable, suitable when:
 * <ul>
 *   <li>The failure cause has a consistent recovery time</li>
 *   <li>Load spreading is not a concern</li>
 *   <li>Predictable retry timing is preferred</li>
 * </ul>
 *
 * <p>Example:
 * <pre>{@code
 * RetryPolicy policy = FixedDelay.of(Duration.ofSeconds(5), 3);
 * // Retries at: 5s, 5s, 5s (then gives up)
 * }</pre>
 */
public final class FixedDelay implements RetryPolicy {

    private final Duration delay;
    private final int maxAttempts;

    private FixedDelay(Duration delay, int maxAttempts) {
        this.delay = delay;
        this.maxAttempts = maxAttempts;
    }

    /**
     * Creates a fixed delay retry policy.
     *
     * @param delay the delay between attempts
     * @param maxAttempts the maximum number of attempts
     * @return a new FixedDelay policy
     * @throws NullPointerException if delay is null
     * @throws IllegalArgumentException if delay is negative or maxAttempts is not positive
     */
    public static FixedDelay of(Duration delay, int maxAttempts) {
        Objects.requireNonNull(delay, "Delay cannot be null");
        if (delay.isNegative()) {
            throw new IllegalArgumentException("Delay cannot be negative");
        }
        if (maxAttempts <= 0) {
            throw new IllegalArgumentException("Max attempts must be positive");
        }
        return new FixedDelay(delay, maxAttempts);
    }

    /**
     * Creates a fixed delay retry policy with default 3 attempts.
     *
     * @param delay the delay between attempts
     * @return a new FixedDelay policy with 3 max attempts
     */
    public static FixedDelay withDelay(Duration delay) {
        return of(delay, 3);
    }

    @Override
    public Optional<Duration> nextRetryDelay(OutboxEvent event, Throwable error) {
        if (!shouldRetry(event) || !isRetryable(error)) {
            return Optional.empty();
        }
        return Optional.of(delay);
    }

    @Override
    public int maxAttempts() {
        return maxAttempts;
    }

    /**
     * Returns the fixed delay between attempts.
     *
     * @return the delay duration
     */
    public Duration delay() {
        return delay;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FixedDelay that)) return false;
        return maxAttempts == that.maxAttempts && Objects.equals(delay, that.delay);
    }

    @Override
    public int hashCode() {
        return Objects.hash(delay, maxAttempts);
    }
}
