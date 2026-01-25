package xyz.charks.outbox.spring;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Configuration properties for the Outbox library.
 *
 * <p>Properties can be set in application.yml or application.properties:
 * <pre>
 * charks:
 *   outbox:
 *     enabled: true
 *     relay:
 *       enabled: true
 *       poll-interval: 1s
 *       batch-size: 100
 *       shutdown-timeout: 30s
 *     retry:
 *       max-attempts: 3
 *       base-delay: 1s
 *       max-delay: 1m
 *       jitter-factor: 0.1
 *     table:
 *       name: outbox_events
 *       schema: public
 * </pre>
 */
@ConfigurationProperties(prefix = "charks.outbox")
public class OutboxProperties {

    /**
     * Whether the outbox functionality is enabled.
     */
    private boolean enabled = true;

    /**
     * Relay configuration.
     */
    private final Relay relay = new Relay();

    /**
     * Retry policy configuration.
     */
    private final Retry retry = new Retry();

    /**
     * Database table configuration.
     */
    private final Table table = new Table();

    /**
     * Kafka connector configuration.
     */
    private final Kafka kafka = new Kafka();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public Relay getRelay() {
        return relay;
    }

    public Retry getRetry() {
        return retry;
    }

    public Table getTable() {
        return table;
    }

    public Kafka getKafka() {
        return kafka;
    }

    /**
     * Relay (polling) configuration.
     */
    public static class Relay {

        /**
         * Whether the relay is enabled.
         */
        private boolean enabled = true;

        /**
         * Interval between polling cycles.
         */
        private Duration pollInterval = Duration.ofSeconds(1);

        /**
         * Maximum number of events to process per batch.
         */
        private int batchSize = 100;

        /**
         * Timeout for graceful shutdown.
         */
        private Duration shutdownTimeout = Duration.ofSeconds(30);

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public Duration getPollInterval() {
            return pollInterval;
        }

        public void setPollInterval(Duration pollInterval) {
            this.pollInterval = pollInterval;
        }

        public int getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }

        public Duration getShutdownTimeout() {
            return shutdownTimeout;
        }

        public void setShutdownTimeout(Duration shutdownTimeout) {
            this.shutdownTimeout = shutdownTimeout;
        }
    }

    /**
     * Retry policy configuration.
     */
    public static class Retry {

        /**
         * Maximum number of retry attempts.
         */
        private int maxAttempts = 3;

        /**
         * Base delay for exponential backoff.
         */
        private Duration baseDelay = Duration.ofSeconds(1);

        /**
         * Maximum delay cap.
         */
        private Duration maxDelay = Duration.ofMinutes(1);

        /**
         * Jitter factor (0 to 1) for randomizing delays.
         */
        private double jitterFactor = 0.1;

        public int getMaxAttempts() {
            return maxAttempts;
        }

        public void setMaxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
        }

        public Duration getBaseDelay() {
            return baseDelay;
        }

        public void setBaseDelay(Duration baseDelay) {
            this.baseDelay = baseDelay;
        }

        public Duration getMaxDelay() {
            return maxDelay;
        }

        public void setMaxDelay(Duration maxDelay) {
            this.maxDelay = maxDelay;
        }

        public double getJitterFactor() {
            return jitterFactor;
        }

        public void setJitterFactor(double jitterFactor) {
            this.jitterFactor = jitterFactor;
        }
    }

    /**
     * Database table configuration.
     */
    public static class Table {

        /**
         * Name of the outbox table.
         */
        private String name = "outbox_events";

        /**
         * Database schema for the outbox table.
         */
        private String schema = "public";

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getSchema() {
            return schema;
        }

        public void setSchema(String schema) {
            this.schema = schema;
        }
    }

    /**
     * Kafka connector configuration.
     */
    public static class Kafka {

        /**
         * Bootstrap servers for Kafka.
         */
        private String bootstrapServers = "localhost:9092";

        /**
         * Acknowledgment mode (none, leader, all).
         */
        private String acks = "all";

        /**
         * Request timeout.
         */
        private Duration requestTimeout = Duration.ofSeconds(30);

        public String getBootstrapServers() {
            return bootstrapServers;
        }

        public void setBootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
        }

        public String getAcks() {
            return acks;
        }

        public void setAcks(String acks) {
            this.acks = acks;
        }

        public Duration getRequestTimeout() {
            return requestTimeout;
        }

        public void setRequestTimeout(Duration requestTimeout) {
            this.requestTimeout = requestTimeout;
        }
    }
}
