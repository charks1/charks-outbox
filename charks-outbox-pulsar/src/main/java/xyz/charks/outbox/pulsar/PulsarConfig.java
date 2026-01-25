package xyz.charks.outbox.pulsar;

import org.apache.pulsar.client.api.PulsarClient;
import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * Configuration for the Pulsar broker connector.
 *
 * @see PulsarBrokerConnector
 */
public final class PulsarConfig {

    private static final String DEFAULT_TOPIC_PREFIX = "persistent://public/default/outbox-";

    private final PulsarClient client;
    private final String topicPrefix;
    private final boolean enableBatching;
    private final int batchingMaxMessages;

    private PulsarConfig(Builder builder) {
        this.client = Objects.requireNonNull(builder.client, "client");
        this.topicPrefix = builder.topicPrefix != null ? builder.topicPrefix : DEFAULT_TOPIC_PREFIX;
        this.enableBatching = builder.enableBatching;
        this.batchingMaxMessages = builder.batchingMaxMessages;
    }

    /**
     * Returns the Pulsar client.
     *
     * @return the Pulsar client
     */
    public PulsarClient client() {
        return client;
    }

    /**
     * Returns the topic prefix.
     *
     * @return the topic prefix
     */
    public String topicPrefix() {
        return topicPrefix;
    }

    /**
     * Returns whether batching is enabled.
     *
     * @return true if batching is enabled
     */
    public boolean enableBatching() {
        return enableBatching;
    }

    /**
     * Returns the maximum number of messages in a batch.
     *
     * @return the maximum batch size
     */
    public int batchingMaxMessages() {
        return batchingMaxMessages;
    }

    /**
     * Creates a new builder.
     *
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link PulsarConfig}.
     */
    public static final class Builder {

        private @Nullable PulsarClient client;
        private @Nullable String topicPrefix;
        private boolean enableBatching = true;
        private int batchingMaxMessages = 1000;

        private Builder() {
        }

        /**
         * Sets the Pulsar client.
         *
         * @param client the Pulsar client
         * @return this builder
         */
        public Builder client(PulsarClient client) {
            this.client = client;
            return this;
        }

        /**
         * Sets the topic prefix.
         *
         * @param topicPrefix the topic prefix
         * @return this builder
         */
        public Builder topicPrefix(String topicPrefix) {
            this.topicPrefix = topicPrefix;
            return this;
        }

        /**
         * Sets whether batching is enabled.
         *
         * @param enableBatching true to enable batching
         * @return this builder
         */
        public Builder enableBatching(boolean enableBatching) {
            this.enableBatching = enableBatching;
            return this;
        }

        /**
         * Sets the maximum number of messages in a batch.
         *
         * @param batchingMaxMessages the maximum batch size
         * @return this builder
         */
        public Builder batchingMaxMessages(int batchingMaxMessages) {
            this.batchingMaxMessages = batchingMaxMessages;
            return this;
        }

        /**
         * Builds the configuration.
         *
         * @return a new PulsarConfig
         */
        public PulsarConfig build() {
            return new PulsarConfig(this);
        }
    }
}
