package xyz.charks.outbox.quarkus;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;

import java.time.Duration;
import java.util.Optional;

/**
 * Quarkus configuration mapping for the Charks Outbox library.
 *
 * <p>Configuration properties are prefixed with {@code charks.outbox}.
 *
 * <p>Example configuration in application.properties:
 * <pre>
 * charks.outbox.enabled=true
 * charks.outbox.relay.batch-size=100
 * charks.outbox.relay.polling-interval=1s
 * </pre>
 */
@ConfigMapping(prefix = "charks.outbox")
public interface OutboxConfiguration {

    /**
     * Whether the outbox relay is enabled.
     *
     * @return true if enabled
     */
    @WithDefault("true")
    boolean enabled();

    /**
     * Relay configuration.
     *
     * @return the relay configuration
     */
    RelayConfiguration relay();

    /**
     * Retry configuration.
     *
     * @return the retry configuration
     */
    RetryConfiguration retry();

    /**
     * Kafka configuration.
     *
     * @return the Kafka configuration
     */
    Optional<KafkaConfiguration> kafka();

    /**
     * Relay-specific configuration.
     */
    interface RelayConfiguration {

        /**
         * Maximum number of events to process per poll cycle.
         *
         * @return the batch size
         */
        @WithName("batch-size")
        @WithDefault("100")
        int batchSize();

        /**
         * Interval between poll cycles.
         *
         * @return the polling interval
         */
        @WithName("polling-interval")
        @WithDefault("1s")
        Duration pollingInterval();

        /**
         * Timeout for graceful shutdown.
         *
         * @return the shutdown timeout
         */
        @WithName("shutdown-timeout")
        @WithDefault("30s")
        Duration shutdownTimeout();
    }

    /**
     * Retry-specific configuration.
     */
    interface RetryConfiguration {

        /**
         * Maximum number of retry attempts.
         *
         * @return the maximum attempts
         */
        @WithName("max-attempts")
        @WithDefault("10")
        int maxAttempts();

        /**
         * Initial delay before first retry.
         *
         * @return the initial delay
         */
        @WithName("initial-delay")
        @WithDefault("1s")
        Duration initialDelay();

        /**
         * Maximum delay between retries.
         *
         * @return the maximum delay
         */
        @WithName("max-delay")
        @WithDefault("5m")
        Duration maxDelay();

        /**
         * Multiplier for exponential backoff.
         *
         * @return the multiplier
         */
        @WithDefault("2.0")
        double multiplier();
    }

    /**
     * Kafka-specific configuration.
     */
    interface KafkaConfiguration {

        /**
         * Kafka bootstrap servers.
         *
         * @return the bootstrap servers
         */
        @WithName("bootstrap-servers")
        String bootstrapServers();

        /**
         * Topic name prefix for outbox events.
         *
         * @return the topic prefix
         */
        @WithName("topic-prefix")
        @WithDefault("outbox.")
        String topicPrefix();
    }
}
