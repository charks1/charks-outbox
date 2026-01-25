package xyz.charks.outbox.sqs;

import org.jspecify.annotations.Nullable;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.util.Objects;

/**
 * Configuration for the AWS SQS broker connector.
 *
 * @see SqsBrokerConnector
 */
public final class SqsConfig {

    private final SqsClient client;
    private final String queueUrl;
    private final int delaySeconds;

    private SqsConfig(Builder builder) {
        this.client = Objects.requireNonNull(builder.client, "client");
        this.queueUrl = Objects.requireNonNull(builder.queueUrl, "queueUrl");
        this.delaySeconds = builder.delaySeconds;
    }

    /**
     * Returns the SQS client.
     *
     * @return the SQS client
     */
    public SqsClient client() {
        return client;
    }

    /**
     * Returns the queue URL.
     *
     * @return the queue URL
     */
    public String queueUrl() {
        return queueUrl;
    }

    /**
     * Returns the delay in seconds before the message is available.
     *
     * @return the delay in seconds
     */
    public int delaySeconds() {
        return delaySeconds;
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
     * Builder for {@link SqsConfig}.
     */
    public static final class Builder {

        private @Nullable SqsClient client;
        private @Nullable String queueUrl;
        private int delaySeconds = 0;

        private Builder() {
        }

        /**
         * Sets the SQS client.
         *
         * @param client the SQS client
         * @return this builder
         */
        public Builder client(SqsClient client) {
            this.client = client;
            return this;
        }

        /**
         * Sets the queue URL.
         *
         * @param queueUrl the queue URL
         * @return this builder
         */
        public Builder queueUrl(String queueUrl) {
            this.queueUrl = queueUrl;
            return this;
        }

        /**
         * Sets the delay in seconds before the message is available.
         *
         * @param delaySeconds the delay in seconds (0-900)
         * @return this builder
         */
        public Builder delaySeconds(int delaySeconds) {
            this.delaySeconds = delaySeconds;
            return this;
        }

        /**
         * Builds the configuration.
         *
         * @return a new SqsConfig
         */
        public SqsConfig build() {
            return new SqsConfig(this);
        }
    }
}
