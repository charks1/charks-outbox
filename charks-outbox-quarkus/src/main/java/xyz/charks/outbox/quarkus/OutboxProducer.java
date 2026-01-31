package xyz.charks.outbox.quarkus;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.charks.outbox.broker.BrokerConnector;
import xyz.charks.outbox.core.OutboxRepository;
import xyz.charks.outbox.relay.OutboxRelay;
import xyz.charks.outbox.relay.RelayConfig;
import xyz.charks.outbox.retry.ExponentialBackoff;

/**
 * CDI producer for outbox components in Quarkus.
 *
 * <p>This class provides CDI beans for the outbox relay and its configuration,
 * integrating with the Quarkus configuration system.
 */
@ApplicationScoped
public class OutboxProducer {

    private static final Logger LOG = LoggerFactory.getLogger(OutboxProducer.class);

    private final OutboxConfiguration config;
    private final Instance<OutboxRepository> repositoryInstance;
    private final Instance<BrokerConnector> connectorInstance;

    /**
     * Creates a new outbox producer.
     *
     * @param config the outbox configuration
     * @param repositoryInstance the repository instance provider
     * @param connectorInstance the broker connector instance provider
     */
    @Inject
    public OutboxProducer(
            OutboxConfiguration config,
            Instance<OutboxRepository> repositoryInstance,
            Instance<BrokerConnector> connectorInstance) {
        this.config = config;
        this.repositoryInstance = repositoryInstance;
        this.connectorInstance = connectorInstance;
    }

    /**
     * Produces the relay configuration from Quarkus config.
     *
     * @return the relay configuration
     */
    @Produces
    @ApplicationScoped
    public RelayConfig relayConfig() {
        var relayConfig = config.relay();
        var retryConfig = config.retry();

        return RelayConfig.builder()
            .batchSize(relayConfig.batchSize())
            .pollingInterval(relayConfig.pollingInterval())
            .shutdownTimeout(relayConfig.shutdownTimeout())
            .retryPolicy(ExponentialBackoff.builder()
                .maxAttempts(retryConfig.maxAttempts())
                .baseDelay(retryConfig.initialDelay())
                .maxDelay(retryConfig.maxDelay())
                .build())
            .build();
    }

    /**
     * Produces the outbox relay if enabled and all dependencies are available.
     *
     * @param relayConfig the relay configuration
     * @return the outbox relay, or null if disabled
     */
    @Produces
    @ApplicationScoped
    public @Nullable OutboxRelay outboxRelay(RelayConfig relayConfig) {
        if (!config.enabled()) {
            LOG.info("Outbox relay is disabled");
            return null;
        }

        if (repositoryInstance.isUnsatisfied()) {
            LOG.warn("OutboxRepository not available, relay will not be created");
            return null;
        }

        if (connectorInstance.isUnsatisfied()) {
            LOG.warn("BrokerConnector not available, relay will not be created");
            return null;
        }

        OutboxRelay relay = new OutboxRelay(
            repositoryInstance.get(),
            connectorInstance.get(),
            relayConfig
        );

        LOG.info("Created OutboxRelay with batch size {} and polling interval {}",
            relayConfig.batchSize(), relayConfig.pollingInterval());

        return relay;
    }
}
