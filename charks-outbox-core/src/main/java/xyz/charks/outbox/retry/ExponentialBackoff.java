package xyz.charks.outbox.retry;

import xyz.charks.outbox.core.OutboxEvent;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Retry policy with exponential backoff between attempts.
 *
 * <p>The delay doubles with each attempt, up to a maximum cap:
 * <pre>
 * delay = min(baseDelay * 2^attempt, maxDelay) * (1 + random(0, jitterFactor))
 * </pre>
 *
 * <p>Example with base delay of 1 second and max of 1 minute:
 * <ul>
 *   <li>Attempt 1: ~1 second</li>
 *   <li>Attempt 2: ~2 seconds</li>
 *   <li>Attempt 3: ~4 seconds</li>
 *   <li>Attempt 4: ~8 seconds</li>
 *   <li>Attempt 5+: capped at ~1 minute</li>
 * </ul>
 *
 * <p>Use the builder for configuration:
 * <pre>{@code
 * RetryPolicy policy = ExponentialBackoff.builder()
 *     .maxAttempts(5)
 *     .baseDelay(Duration.ofSeconds(1))
 *     .maxDelay(Duration.ofMinutes(5))
 *     .jitterFactor(0.1)
 *     .build();
 * }</pre>
 */
public final class ExponentialBackoff implements RetryPolicy {

    private final int maxAttempts;
    private final Duration baseDelay;
    private final Duration maxDelay;
    private final double jitterFactor;

    private ExponentialBackoff(int maxAttempts, Duration baseDelay, Duration maxDelay, double jitterFactor) {
        this.maxAttempts = maxAttempts;
        this.baseDelay = baseDelay;
        this.maxDelay = maxDelay;
        this.jitterFactor = jitterFactor;
    }

    /**
     * Creates a default exponential backoff policy.
     *
     * <p>Defaults: 3 max attempts, 1 second base delay, 1 minute max delay, 10% jitter.
     *
     * @return a default exponential backoff policy
     */
    public static ExponentialBackoff defaults() {
        return builder().build();
    }

    /**
     * Creates a builder for configuring the exponential backoff policy.
     *
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public Optional<Duration> nextRetryDelay(OutboxEvent event, Throwable error) {
        if (!shouldRetry(event) || !isRetryable(error)) {
            return Optional.empty();
        }

        int attempt = event.retryCount();
        long delayMillis = baseDelay.toMillis() * (1L << Math.min(attempt, 30));
        delayMillis = Math.min(delayMillis, maxDelay.toMillis());

        if (jitterFactor > 0) {
            double jitter = ThreadLocalRandom.current().nextDouble(0, jitterFactor);
            delayMillis = (long) (delayMillis * (1 + jitter));
        }

        return Optional.of(Duration.ofMillis(delayMillis));
    }

    @Override
    public int maxAttempts() {
        return maxAttempts;
    }

    /**
     * Returns the base delay for the first retry.
     *
     * @return the base delay
     */
    public Duration baseDelay() {
        return baseDelay;
    }

    /**
     * Returns the maximum delay cap.
     *
     * @return the maximum delay
     */
    public Duration maxDelay() {
        return maxDelay;
    }

    /**
     * Returns the jitter factor (0 to 1).
     *
     * @return the jitter factor
     */
    public double jitterFactor() {
        return jitterFactor;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ExponentialBackoff that)) return false;
        return maxAttempts == that.maxAttempts
                && Double.compare(jitterFactor, that.jitterFactor) == 0
                && Objects.equals(baseDelay, that.baseDelay)
                && Objects.equals(maxDelay, that.maxDelay);
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxAttempts, baseDelay, maxDelay, jitterFactor);
    }

    /**
     * Builder for ExponentialBackoff policy.
     */
    public static final class Builder {
        private int maxAttempts = 3;
        private Duration baseDelay = Duration.ofSeconds(1);
        private Duration maxDelay = Duration.ofMinutes(1);
        private double jitterFactor = 0.1;

        private Builder() {}

        /**
         * Sets the maximum number of retry attempts.
         *
         * @param maxAttempts the maximum attempts (must be positive)
         * @return this builder
         */
        public Builder maxAttempts(int maxAttempts) {
            if (maxAttempts <= 0) {
                throw new IllegalArgumentException("Max attempts must be positive");
            }
            this.maxAttempts = maxAttempts;
            return this;
        }

        /**
         * Sets the base delay for the first retry.
         *
         * @param baseDelay the base delay (must not be negative)
         * @return this builder
         */
        public Builder baseDelay(Duration baseDelay) {
            Objects.requireNonNull(baseDelay, "Base delay cannot be null");
            if (baseDelay.isNegative()) {
                throw new IllegalArgumentException("Base delay cannot be negative");
            }
            this.baseDelay = baseDelay;
            return this;
        }

        /**
         * Sets the maximum delay cap.
         *
         * @param maxDelay the maximum delay (must not be negative)
         * @return this builder
         */
        public Builder maxDelay(Duration maxDelay) {
            Objects.requireNonNull(maxDelay, "Max delay cannot be null");
            if (maxDelay.isNegative()) {
                throw new IllegalArgumentException("Max delay cannot be negative");
            }
            this.maxDelay = maxDelay;
            return this;
        }

        /**
         * Sets the jitter factor.
         *
         * <p>A jitter of 0.1 adds up to 10% random variation to delays,
         * helping spread out retry storms in distributed systems.
         *
         * @param jitterFactor the jitter factor (0 to 1)
         * @return this builder
         */
        public Builder jitterFactor(double jitterFactor) {
            if (jitterFactor < 0 || jitterFactor > 1) {
                throw new IllegalArgumentException("Jitter factor must be between 0 and 1");
            }
            this.jitterFactor = jitterFactor;
            return this;
        }

        /**
         * Builds the ExponentialBackoff policy.
         *
         * @return a new ExponentialBackoff instance
         */
        public ExponentialBackoff build() {
            return new ExponentialBackoff(maxAttempts, baseDelay, maxDelay, jitterFactor);
        }
    }
}
