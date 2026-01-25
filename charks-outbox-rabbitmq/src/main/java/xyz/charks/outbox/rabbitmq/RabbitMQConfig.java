package xyz.charks.outbox.rabbitmq;

import com.rabbitmq.client.ConnectionFactory;
import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * Configuration for the RabbitMQ broker connector.
 *
 * @see RabbitMQBrokerConnector
 */
public final class RabbitMQConfig {

    private static final String DEFAULT_EXCHANGE = "outbox.events";
    private static final String DEFAULT_ROUTING_KEY_PREFIX = "outbox.";

    private final ConnectionFactory connectionFactory;
    private final String exchange;
    private final String routingKeyPrefix;
    private final boolean mandatory;
    private final boolean persistent;

    private RabbitMQConfig(Builder builder) {
        this.connectionFactory = Objects.requireNonNull(builder.connectionFactory, "connectionFactory");
        this.exchange = builder.exchange != null ? builder.exchange : DEFAULT_EXCHANGE;
        this.routingKeyPrefix = builder.routingKeyPrefix != null ? builder.routingKeyPrefix : DEFAULT_ROUTING_KEY_PREFIX;
        this.mandatory = builder.mandatory;
        this.persistent = builder.persistent;
    }

    /**
     * Returns the RabbitMQ connection factory.
     *
     * @return the connection factory
     */
    public ConnectionFactory connectionFactory() {
        return connectionFactory;
    }

    /**
     * Returns the exchange name.
     *
     * @return the exchange name
     */
    public String exchange() {
        return exchange;
    }

    /**
     * Returns the routing key prefix.
     *
     * @return the routing key prefix
     */
    public String routingKeyPrefix() {
        return routingKeyPrefix;
    }

    /**
     * Returns whether messages should be published as mandatory.
     *
     * @return true if mandatory
     */
    public boolean mandatory() {
        return mandatory;
    }

    /**
     * Returns whether messages should be persistent.
     *
     * @return true if persistent
     */
    public boolean persistent() {
        return persistent;
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
     * Builder for {@link RabbitMQConfig}.
     */
    public static final class Builder {

        private @Nullable ConnectionFactory connectionFactory;
        private @Nullable String exchange;
        private @Nullable String routingKeyPrefix;
        private boolean mandatory = false;
        private boolean persistent = true;

        private Builder() {
        }

        /**
         * Sets the connection factory.
         *
         * @param connectionFactory the connection factory
         * @return this builder
         */
        public Builder connectionFactory(ConnectionFactory connectionFactory) {
            this.connectionFactory = connectionFactory;
            return this;
        }

        /**
         * Sets the exchange name.
         *
         * @param exchange the exchange name
         * @return this builder
         */
        public Builder exchange(String exchange) {
            this.exchange = exchange;
            return this;
        }

        /**
         * Sets the routing key prefix.
         *
         * @param routingKeyPrefix the routing key prefix
         * @return this builder
         */
        public Builder routingKeyPrefix(String routingKeyPrefix) {
            this.routingKeyPrefix = routingKeyPrefix;
            return this;
        }

        /**
         * Sets whether messages should be published as mandatory.
         *
         * @param mandatory true if mandatory
         * @return this builder
         */
        public Builder mandatory(boolean mandatory) {
            this.mandatory = mandatory;
            return this;
        }

        /**
         * Sets whether messages should be persistent.
         *
         * @param persistent true if persistent
         * @return this builder
         */
        public Builder persistent(boolean persistent) {
            this.persistent = persistent;
            return this;
        }

        /**
         * Builds the configuration.
         *
         * @return a new RabbitMQConfig
         */
        public RabbitMQConfig build() {
            return new RabbitMQConfig(this);
        }
    }
}
