package xyz.charks.outbox.r2dbc;

import io.r2dbc.spi.ConnectionFactory;
import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * Configuration for the R2DBC outbox repository.
 *
 * <p>Use the {@link Builder} to create instances with custom settings.
 *
 * @see R2dbcOutboxRepository
 */
public final class R2dbcOutboxConfig {

    private static final String DEFAULT_TABLE_NAME = "outbox_events";

    private final ConnectionFactory connectionFactory;
    private final String tableName;

    private R2dbcOutboxConfig(Builder builder) {
        this.connectionFactory = Objects.requireNonNull(builder.connectionFactory, "connectionFactory");
        this.tableName = builder.tableName != null ? builder.tableName : DEFAULT_TABLE_NAME;
    }

    /**
     * Returns the R2DBC connection factory.
     *
     * @return the connection factory
     */
    public ConnectionFactory connectionFactory() {
        return connectionFactory;
    }

    /**
     * Returns the table name.
     *
     * @return the table name
     */
    public String tableName() {
        return tableName;
    }

    /**
     * Creates a new builder for R2dbcOutboxConfig.
     *
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link R2dbcOutboxConfig}.
     */
    public static final class Builder {

        private @Nullable ConnectionFactory connectionFactory;
        private @Nullable String tableName;

        private Builder() {
        }

        /**
         * Sets the R2DBC connection factory.
         *
         * @param connectionFactory the connection factory
         * @return this builder
         */
        public Builder connectionFactory(ConnectionFactory connectionFactory) {
            this.connectionFactory = connectionFactory;
            return this;
        }

        /**
         * Sets the table name.
         *
         * @param tableName the table name
         * @return this builder
         */
        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        /**
         * Builds the configuration.
         *
         * @return a new R2dbcOutboxConfig
         */
        public R2dbcOutboxConfig build() {
            return new R2dbcOutboxConfig(this);
        }
    }
}
