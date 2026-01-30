package xyz.charks.outbox.kafka;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Configuration for the Kafka broker connector.
 *
 * <p>Provides configuration options for the Kafka producer including:
 * <ul>
 *   <li>Bootstrap servers connection string</li>
 *   <li>Producer timeouts and retries</li>
 *   <li>Acknowledgment mode</li>
 *   <li>Additional producer properties</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * KafkaConfig config = KafkaConfig.builder()
 *     .bootstrapServers("localhost:9092")
 *     .acks(Acks.ALL)
 *     .requestTimeout(Duration.ofSeconds(30))
 *     .build();
 * }</pre>
 *
 * @param bootstrapServers comma-separated list of broker addresses
 * @param acks acknowledgment mode (0, 1, or all)
 * @param requestTimeout timeout for producer requests
 * @param retries number of retries for failed sends
 * @param lingerMs time to wait for additional messages before sending a batch
 * @param batchSize maximum size of a batch in bytes
 * @param bufferMemory total memory for buffering records
 * @param compressionType compression algorithm (none, gzip, snappy, lz4, zstd)
 * @param additionalProperties extra producer configuration properties
 */
public record KafkaConfig(
        String bootstrapServers,
        Acks acks,
        Duration requestTimeout,
        int retries,
        int lingerMs,
        int batchSize,
        long bufferMemory,
        String compressionType,
        Map<String, Object> additionalProperties
) {

    /**
     * Acknowledgment modes for Kafka producer.
     */
    public enum Acks {
        /** No acknowledgment (fire and forget) */
        NONE("0"),
        /** Leader acknowledgment only */
        LEADER("1"),
        /** Full ISR acknowledgment */
        ALL("all");

        private final String value;

        Acks(String value) {
            this.value = value;
        }

        /**
         * Returns the Kafka configuration value.
         *
         * @return the acks configuration value
         */
        public String value() {
            return value;
        }
    }

    /**
     * Creates a KafkaConfig with validation.
     *
     * @throws NullPointerException if required fields are null
     * @throws IllegalArgumentException if bootstrapServers is blank
     */
    public KafkaConfig {
        Objects.requireNonNull(bootstrapServers, "Bootstrap servers cannot be null");
        Objects.requireNonNull(acks, "Acks cannot be null");
        Objects.requireNonNull(requestTimeout, "Request timeout cannot be null");
        Objects.requireNonNull(compressionType, "Compression type cannot be null");
        Objects.requireNonNull(additionalProperties, "Additional properties cannot be null");

        if (bootstrapServers.isBlank()) {
            throw new IllegalArgumentException("Bootstrap servers cannot be blank");
        }
        if (retries < 0) {
            throw new IllegalArgumentException("Retries cannot be negative");
        }

        additionalProperties = Map.copyOf(additionalProperties);
    }

    /**
     * Creates a configuration for localhost development.
     *
     * @return a config pointing to localhost:9092
     */
    public static KafkaConfig localhost() {
        return builder().bootstrapServers("localhost:9092").build();
    }

    /**
     * Creates a new builder for KafkaConfig.
     *
     * @return a new builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Converts this configuration to Kafka producer properties.
     *
     * @return a map of producer configuration properties
     */
    public Map<String, Object> toProducerProperties() {
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("acks", acks.value());
        props.put("request.timeout.ms", (int) requestTimeout.toMillis());
        props.put("retries", retries);
        props.put("linger.ms", lingerMs);
        props.put("batch.size", batchSize);
        props.put("buffer.memory", bufferMemory);
        props.put("compression.type", compressionType);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.putAll(additionalProperties);
        return props;
    }

    /**
     * Builder for KafkaConfig.
     */
    public static final class Builder {
        private String bootstrapServers = "localhost:9092";
        private Acks acks = Acks.ALL;
        private Duration requestTimeout = Duration.ofSeconds(30);
        private int retries = 3;
        private int lingerMs = 5;
        private int batchSize = 16384;
        private long bufferMemory = 33554432L;
        private String compressionType = "none";
        private final Map<String, Object> additionalProperties = new HashMap<>();

        private Builder() {}

        /**
         * Sets the bootstrap servers.
         *
         * @param bootstrapServers comma-separated list of broker addresses
         * @return this builder
         */
        public Builder bootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
            return this;
        }

        /**
         * Sets the acknowledgment mode.
         *
         * @param acks the acknowledgment mode
         * @return this builder
         */
        public Builder acks(Acks acks) {
            this.acks = acks;
            return this;
        }

        /**
         * Sets the request timeout.
         *
         * @param requestTimeout the timeout duration
         * @return this builder
         */
        public Builder requestTimeout(Duration requestTimeout) {
            this.requestTimeout = requestTimeout;
            return this;
        }

        /**
         * Sets the number of retries.
         *
         * @param retries number of retry attempts
         * @return this builder
         */
        public Builder retries(int retries) {
            this.retries = retries;
            return this;
        }

        /**
         * Sets the linger time in milliseconds.
         *
         * @param lingerMs time to wait before sending a batch
         * @return this builder
         */
        public Builder lingerMs(int lingerMs) {
            this.lingerMs = lingerMs;
            return this;
        }

        /**
         * Sets the batch size in bytes.
         *
         * @param batchSize maximum batch size
         * @return this builder
         */
        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        /**
         * Sets the buffer memory in bytes.
         *
         * @param bufferMemory total buffer memory
         * @return this builder
         */
        public Builder bufferMemory(long bufferMemory) {
            this.bufferMemory = bufferMemory;
            return this;
        }

        /**
         * Sets the compression type.
         *
         * @param compressionType compression algorithm (none, gzip, snappy, lz4, zstd)
         * @return this builder
         */
        public Builder compressionType(String compressionType) {
            this.compressionType = compressionType;
            return this;
        }

        /**
         * Adds an additional producer property.
         *
         * @param key the property key
         * @param value the property value
         * @return this builder
         */
        public Builder property(String key, Object value) {
            this.additionalProperties.put(key, value);
            return this;
        }

        /**
         * Adds multiple additional producer properties.
         *
         * @param properties the properties to add
         * @return this builder
         */
        public Builder properties(Map<String, Object> properties) {
            this.additionalProperties.putAll(properties);
            return this;
        }

        /**
         * Builds the KafkaConfig.
         *
         * @return a new KafkaConfig instance
         */
        public KafkaConfig build() {
            return new KafkaConfig(
                    bootstrapServers,
                    acks,
                    requestTimeout,
                    retries,
                    lingerMs,
                    batchSize,
                    bufferMemory,
                    compressionType,
                    additionalProperties
            );
        }
    }
}
