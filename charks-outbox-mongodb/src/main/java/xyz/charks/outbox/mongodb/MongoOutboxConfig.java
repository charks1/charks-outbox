package xyz.charks.outbox.mongodb;

import com.mongodb.client.MongoClient;
import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * Configuration for the MongoDB outbox repository.
 *
 * @see MongoOutboxRepository
 */
public final class MongoOutboxConfig {

    private static final String DEFAULT_DATABASE = "outbox";
    private static final String DEFAULT_COLLECTION = "outbox_events";

    private final MongoClient client;
    private final String database;
    private final String collection;

    private MongoOutboxConfig(Builder builder) {
        this.client = Objects.requireNonNull(builder.client, "client");
        this.database = builder.database != null ? builder.database : DEFAULT_DATABASE;
        this.collection = builder.collection != null ? builder.collection : DEFAULT_COLLECTION;
    }

    /**
     * Returns the MongoDB client.
     *
     * @return the MongoDB client
     */
    public MongoClient client() {
        return client;
    }

    /**
     * Returns the database name.
     *
     * @return the database name
     */
    public String database() {
        return database;
    }

    /**
     * Returns the collection name.
     *
     * @return the collection name
     */
    public String collection() {
        return collection;
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
     * Builder for {@link MongoOutboxConfig}.
     */
    public static final class Builder {

        private @Nullable MongoClient client;
        private @Nullable String database;
        private @Nullable String collection;

        private Builder() {
        }

        /**
         * Sets the MongoDB client.
         *
         * @param client the MongoDB client
         * @return this builder
         */
        public Builder client(MongoClient client) {
            this.client = client;
            return this;
        }

        /**
         * Sets the database name.
         *
         * @param database the database name
         * @return this builder
         */
        public Builder database(String database) {
            this.database = database;
            return this;
        }

        /**
         * Sets the collection name.
         *
         * @param collection the collection name
         * @return this builder
         */
        public Builder collection(String collection) {
            this.collection = collection;
            return this;
        }

        /**
         * Builds the configuration.
         *
         * @return a new MongoOutboxConfig
         */
        public MongoOutboxConfig build() {
            return new MongoOutboxConfig(this);
        }
    }
}
