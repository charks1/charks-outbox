package xyz.charks.outbox.spring;

import xyz.charks.outbox.broker.BrokerConnector;
import xyz.charks.outbox.core.OutboxRepository;
import xyz.charks.outbox.kafka.KafkaBrokerConnector;
import xyz.charks.outbox.kafka.KafkaConfig;
import xyz.charks.outbox.relay.OutboxRelay;
import xyz.charks.outbox.relay.RelayConfig;
import xyz.charks.outbox.retry.ExponentialBackoff;
import xyz.charks.outbox.retry.RetryPolicy;
import xyz.charks.outbox.serializer.Serializer;
import xyz.charks.outbox.serializer.json.JacksonSerializer;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Auto-configuration for the Charks Outbox library.
 *
 * <p>Automatically configures:
 * <ul>
 *   <li>Retry policy based on properties</li>
 *   <li>JSON serializer if Jackson is available</li>
 *   <li>Kafka connector if kafka-clients is on classpath</li>
 *   <li>Outbox relay if repository and connector are available</li>
 * </ul>
 *
 * <p>The auto-configuration can be disabled by setting:
 * <pre>charks.outbox.enabled=false</pre>
 */
@AutoConfiguration
@EnableConfigurationProperties(OutboxProperties.class)
@ConditionalOnProperty(prefix = "charks.outbox", name = "enabled", havingValue = "true", matchIfMissing = true)
public class OutboxAutoConfiguration {

    /**
     * Creates the default retry policy using exponential backoff.
     *
     * @param properties the outbox properties
     * @return the configured retry policy
     */
    @Bean
    @ConditionalOnMissingBean
    public RetryPolicy outboxRetryPolicy(OutboxProperties properties) {
        OutboxProperties.Retry retry = properties.getRetry();
        return ExponentialBackoff.builder()
                .maxAttempts(retry.getMaxAttempts())
                .baseDelay(retry.getBaseDelay())
                .maxDelay(retry.getMaxDelay())
                .jitterFactor(retry.getJitterFactor())
                .build();
    }

    /**
     * JSON serializer auto-configuration.
     */
    @AutoConfiguration
    @ConditionalOnClass(JacksonSerializer.class)
    public static class JsonSerializerAutoConfiguration {

        /**
         * Creates the default JSON serializer.
         *
         * @return a Jackson-based serializer
         */
        @Bean
        @ConditionalOnMissingBean
        public Serializer<Object> outboxSerializer() {
            return JacksonSerializer.create();
        }
    }

    /**
     * Kafka connector auto-configuration.
     */
    @AutoConfiguration
    @ConditionalOnClass(KafkaBrokerConnector.class)
    @ConditionalOnProperty(prefix = "charks.outbox.kafka", name = "bootstrap-servers")
    public static class KafkaConnectorAutoConfiguration {

        /**
         * Creates the Kafka connector.
         *
         * @param properties the outbox properties
         * @return a configured Kafka connector
         */
        @Bean
        @ConditionalOnMissingBean(BrokerConnector.class)
        public KafkaBrokerConnector outboxKafkaConnector(OutboxProperties properties) {
            OutboxProperties.Kafka kafka = properties.getKafka();

            KafkaConfig.Acks acks = switch (kafka.getAcks().toLowerCase()) {
                case "none", "0" -> KafkaConfig.Acks.NONE;
                case "leader", "1" -> KafkaConfig.Acks.LEADER;
                default -> KafkaConfig.Acks.ALL;
            };

            KafkaConfig config = KafkaConfig.builder()
                    .bootstrapServers(kafka.getBootstrapServers())
                    .acks(acks)
                    .requestTimeout(kafka.getRequestTimeout())
                    .build();

            return new KafkaBrokerConnector(config);
        }
    }

    /**
     * Outbox relay auto-configuration.
     */
    @AutoConfiguration
    @ConditionalOnBean({OutboxRepository.class, BrokerConnector.class})
    @ConditionalOnProperty(prefix = "charks.outbox.relay", name = "enabled", havingValue = "true", matchIfMissing = true)
    public static class RelayAutoConfiguration {

        /**
         * Creates the relay configuration.
         *
         * @param properties the outbox properties
         * @param retryPolicy the retry policy
         * @return the relay configuration
         */
        @Bean
        @ConditionalOnMissingBean
        public RelayConfig outboxRelayConfig(OutboxProperties properties, RetryPolicy retryPolicy) {
            OutboxProperties.Relay relay = properties.getRelay();
            return RelayConfig.builder()
                    .pollingInterval(relay.getPollInterval())
                    .batchSize(relay.getBatchSize())
                    .shutdownTimeout(relay.getShutdownTimeout())
                    .retryPolicy(retryPolicy)
                    .build();
        }

        /**
         * Creates the outbox relay.
         *
         * @param repository the outbox repository
         * @param connector the broker connector
         * @param config the relay configuration
         * @return the configured relay
         */
        @Bean(destroyMethod = "stop")
        @ConditionalOnMissingBean
        public OutboxRelay outboxRelay(
                OutboxRepository repository,
                BrokerConnector connector,
                RelayConfig config
        ) {
            OutboxRelay relay = new OutboxRelay(repository, connector, config);
            relay.start();
            return relay;
        }

    }
}
