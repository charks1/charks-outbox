package xyz.charks.outbox.nats;

import io.nats.client.Connection;
import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * Configuration for the NATS broker connector.
 *
 * @see NatsBrokerConnector
 */
public final class NatsConfig {

    private static final String DEFAULT_SUBJECT_PREFIX = "outbox.";

    private final Connection connection;
    private final String subjectPrefix;

    private NatsConfig(Builder builder) {
        this.connection = Objects.requireNonNull(builder.connection, "connection");
        this.subjectPrefix = builder.subjectPrefix != null ? builder.subjectPrefix : DEFAULT_SUBJECT_PREFIX;
    }

    /**
     * Returns the NATS connection.
     *
     * @return the NATS connection
     */
    public Connection connection() {
        return connection;
    }

    /**
     * Returns the subject prefix.
     *
     * @return the subject prefix
     */
    public String subjectPrefix() {
        return subjectPrefix;
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
     * Builder for {@link NatsConfig}.
     */
    public static final class Builder {

        private @Nullable Connection connection;
        private @Nullable String subjectPrefix;

        private Builder() {
        }

        /**
         * Sets the NATS connection.
         *
         * @param connection the NATS connection
         * @return this builder
         */
        public Builder connection(Connection connection) {
            this.connection = connection;
            return this;
        }

        /**
         * Sets the subject prefix.
         *
         * @param subjectPrefix the subject prefix
         * @return this builder
         */
        public Builder subjectPrefix(String subjectPrefix) {
            this.subjectPrefix = subjectPrefix;
            return this;
        }

        /**
         * Builds the configuration.
         *
         * @return a new NatsConfig
         */
        public NatsConfig build() {
            return new NatsConfig(this);
        }
    }
}
