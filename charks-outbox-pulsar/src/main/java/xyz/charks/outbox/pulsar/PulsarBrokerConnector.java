package xyz.charks.outbox.pulsar;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.charks.outbox.broker.BrokerConnector;
import xyz.charks.outbox.broker.PublishResult;
import xyz.charks.outbox.core.OutboxEvent;
import xyz.charks.outbox.exception.OutboxPublishException;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Apache Pulsar implementation of {@link BrokerConnector}.
 *
 * <p>This connector publishes outbox events to Pulsar topics using
 * the aggregate type as the topic name suffix.
 *
 * @see PulsarConfig
 */
public class PulsarBrokerConnector implements BrokerConnector, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarBrokerConnector.class);

    private final PulsarConfig config;
    private final Map<String, Producer<byte[]>> producers = new ConcurrentHashMap<>();

    /**
     * Creates a new Pulsar broker connector.
     *
     * @param config the connector configuration
     */
    public PulsarBrokerConnector(PulsarConfig config) {
        this.config = Objects.requireNonNull(config, "config");
        LOG.info("Initialized Pulsar broker connector with topic prefix: {}", config.topicPrefix());
    }

    @Override
    public PublishResult publish(OutboxEvent event) {
        Objects.requireNonNull(event, "event");

        String topic = buildTopicName(event);

        try {
            Producer<byte[]> producer = getOrCreateProducer(topic);

            TypedMessageBuilder<byte[]> messageBuilder = producer.newMessage()
                .key(event.aggregateId().value())
                .value(event.payload())
                .property("eventId", event.id().value().toString())
                .property("aggregateType", event.aggregateType())
                .property("aggregateId", event.aggregateId().value())
                .property("eventType", event.eventType().value())
                .property("createdAt", event.createdAt().toString());

            event.headers().forEach(messageBuilder::property);

            messageBuilder.send();

            LOG.debug("Published event {} to topic '{}'", event.id(), topic);
            return PublishResult.success(event.id());
        } catch (PulsarClientException e) {
            LOG.error("Failed to publish event {} to Pulsar", event.id(), e);
            return PublishResult.failure(event.id(), e);
        }
    }

    @Override
    public boolean isHealthy() {
        return !config.client().isClosed();
    }

    private String buildTopicName(OutboxEvent event) {
        return config.topicPrefix() + event.aggregateType().toLowerCase();
    }

    private Producer<byte[]> getOrCreateProducer(String topic) throws PulsarClientException {
        return producers.computeIfAbsent(topic, t -> {
            try {
                return config.client().newProducer()
                    .topic(t)
                    .enableBatching(config.enableBatching())
                    .batchingMaxMessages(config.batchingMaxMessages())
                    .create();
            } catch (PulsarClientException e) {
                throw new OutboxPublishException("Failed to create producer for topic: " + t, e);
            }
        });
    }

    @Override
    public void close() {
        for (Producer<byte[]> producer : producers.values()) {
            try {
                producer.close();
            } catch (PulsarClientException e) {
                LOG.warn("Error closing Pulsar producer", e);
            }
        }
        producers.clear();
        LOG.info("Closed Pulsar broker connector");
    }
}
