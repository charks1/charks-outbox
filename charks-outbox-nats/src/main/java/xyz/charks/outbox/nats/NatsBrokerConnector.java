package xyz.charks.outbox.nats;

import io.nats.client.Connection;
import io.nats.client.impl.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.charks.outbox.broker.BrokerConnector;
import xyz.charks.outbox.broker.PublishResult;
import xyz.charks.outbox.core.OutboxEvent;

import java.util.Objects;

/**
 * NATS implementation of {@link BrokerConnector}.
 *
 * <p>This connector publishes outbox events to NATS subjects using
 * the aggregate type and event type as the subject name.
 *
 * @see NatsConfig
 */
public class NatsBrokerConnector implements BrokerConnector, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(NatsBrokerConnector.class);

    private final NatsConfig config;

    /**
     * Creates a new NATS broker connector.
     *
     * @param config the connector configuration
     */
    public NatsBrokerConnector(NatsConfig config) {
        this.config = Objects.requireNonNull(config, "config");
        LOG.info("Initialized NATS broker connector with subject prefix: {}", config.subjectPrefix());
    }

    @Override
    public PublishResult publish(OutboxEvent event) {
        Objects.requireNonNull(event, "event");

        String subject = buildSubject(event);

        try {
            Headers headers = new Headers();
            headers.add("eventId", event.id().value().toString());
            headers.add("aggregateType", event.aggregateType());
            headers.add("aggregateId", event.aggregateId().value());
            headers.add("eventType", event.eventType().value());
            headers.add("createdAt", event.createdAt().toString());
            event.headers().forEach(headers::add);

            config.connection().publish(subject, headers, event.payload());

            LOG.debug("Published event {} to NATS subject '{}'", event.id(), subject);
            return PublishResult.success(event.id());
        } catch (Exception e) {
            LOG.error("Failed to publish event {} to NATS", event.id(), e);
            return PublishResult.failure(event.id(), e);
        }
    }

    @Override
    public boolean isHealthy() {
        return config.connection().getStatus() == Connection.Status.CONNECTED;
    }

    private String buildSubject(OutboxEvent event) {
        return config.subjectPrefix() + event.aggregateType().toLowerCase() + "." + event.eventType().value();
    }

    @Override
    public void close() {
        try {
            if (config.connection().getStatus() == Connection.Status.CONNECTED) {
                config.connection().close();
            }
            LOG.info("Closed NATS broker connector");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted while closing NATS connection", e);
        }
    }
}
