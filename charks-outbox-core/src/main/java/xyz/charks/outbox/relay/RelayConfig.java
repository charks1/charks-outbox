package xyz.charks.outbox.relay;

import xyz.charks.outbox.core.LockMode;
import xyz.charks.outbox.retry.ExponentialBackoff;
import xyz.charks.outbox.retry.RetryPolicy;

import java.time.Duration;
import java.util.Objects;

/**
 * Configuration for the outbox relay.
 *
 * <p>Use the builder for creating custom configurations:
 * <pre>{@code
 * RelayConfig config = RelayConfig.builder()
 *     .batchSize(100)
 *     .pollingInterval(Duration.ofSeconds(1))
 *     .lockMode(LockMode.FOR_UPDATE_SKIP_LOCKED)
 *     .retryPolicy(ExponentialBackoff.defaults())
 *     .build();
 * }</pre>
 *
 * @param batchSize maximum number of events to process per polling cycle
 * @param pollingInterval time between polling cycles
 * @param lockMode database locking strategy for concurrent relays
 * @param retryPolicy strategy for handling failed events
 * @param shutdownTimeout maximum time to wait for graceful shutdown
 * @param useVirtualThreads whether to use virtual threads for event processing
 */
public record RelayConfig(
        int batchSize,
        Duration pollingInterval,
        LockMode lockMode,
        RetryPolicy retryPolicy,
        Duration shutdownTimeout,
        boolean useVirtualThreads
) {

    /**
     * Creates a RelayConfig with validation.
     *
     * @throws IllegalArgumentException if parameters are invalid
     * @throws NullPointerException if required parameters are null
     */
    public RelayConfig {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("Batch size must be positive");
        }
        Objects.requireNonNull(pollingInterval, "Polling interval cannot be null");
        if (pollingInterval.isNegative() || pollingInterval.isZero()) {
            throw new IllegalArgumentException("Polling interval must be positive");
        }
        Objects.requireNonNull(lockMode, "Lock mode cannot be null");
        Objects.requireNonNull(retryPolicy, "Retry policy cannot be null");
        Objects.requireNonNull(shutdownTimeout, "Shutdown timeout cannot be null");
        if (shutdownTimeout.isNegative()) {
            throw new IllegalArgumentException("Shutdown timeout cannot be negative");
        }
    }

    /**
     * Returns the default relay configuration.
     *
     * <p>Defaults:
     * <ul>
     *   <li>Batch size: 100</li>
     *   <li>Polling interval: 1 second</li>
     *   <li>Lock mode: FOR_UPDATE_SKIP_LOCKED</li>
     *   <li>Retry policy: Exponential backoff (3 attempts)</li>
     *   <li>Shutdown timeout: 30 seconds</li>
     *   <li>Virtual threads: enabled</li>
     * </ul>
     *
     * @return the default configuration
     */
    public static RelayConfig defaults() {
        return builder().build();
    }

    /**
     * Creates a new configuration builder.
     *
     * @return a new builder with default values
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for RelayConfig.
     */
    public static final class Builder {
        private int batchSize = 100;
        private Duration pollingInterval = Duration.ofSeconds(1);
        private LockMode lockMode = LockMode.FOR_UPDATE_SKIP_LOCKED;
        private RetryPolicy retryPolicy = ExponentialBackoff.defaults();
        private Duration shutdownTimeout = Duration.ofSeconds(30);
        private boolean useVirtualThreads = true;

        private Builder() {}

        /**
         * Sets the maximum batch size.
         *
         * @param batchSize events per batch (must be positive)
         * @return this builder
         */
        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        /**
         * Sets the polling interval.
         *
         * @param interval time between poll cycles
         * @return this builder
         */
        public Builder pollingInterval(Duration interval) {
            this.pollingInterval = interval;
            return this;
        }

        /**
         * Sets the database locking mode.
         *
         * @param lockMode the locking strategy
         * @return this builder
         */
        public Builder lockMode(LockMode lockMode) {
            this.lockMode = lockMode;
            return this;
        }

        /**
         * Sets the retry policy for failed events.
         *
         * @param retryPolicy the retry strategy
         * @return this builder
         */
        public Builder retryPolicy(RetryPolicy retryPolicy) {
            this.retryPolicy = retryPolicy;
            return this;
        }

        /**
         * Sets the shutdown timeout.
         *
         * @param timeout maximum time to wait for graceful shutdown
         * @return this builder
         */
        public Builder shutdownTimeout(Duration timeout) {
            this.shutdownTimeout = timeout;
            return this;
        }

        /**
         * Enables or disables virtual threads.
         *
         * @param useVirtualThreads true to use virtual threads
         * @return this builder
         */
        public Builder useVirtualThreads(boolean useVirtualThreads) {
            this.useVirtualThreads = useVirtualThreads;
            return this;
        }

        /**
         * Builds the RelayConfig.
         *
         * @return a new RelayConfig instance
         */
        public RelayConfig build() {
            return new RelayConfig(
                    batchSize,
                    pollingInterval,
                    lockMode,
                    retryPolicy,
                    shutdownTimeout,
                    useVirtualThreads
            );
        }
    }
}
