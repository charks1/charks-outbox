package xyz.charks.outbox.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.charks.outbox.broker.BrokerConnector;
import xyz.charks.outbox.broker.PublishResult;
import xyz.charks.outbox.core.OutboxEvent;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

/**
 * RabbitMQ implementation of {@link BrokerConnector}.
 *
 * <p>This connector publishes outbox events to a RabbitMQ exchange using
 * the event type as part of the routing key.
 *
 * @see RabbitMQConfig
 */
public class RabbitMQBrokerConnector implements BrokerConnector, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RabbitMQBrokerConnector.class);

    private final RabbitMQConfig config;
    private final Connection connection;
    private final Channel channel;

    /**
     * Creates a new RabbitMQ broker connector.
     *
     * @param config the connector configuration
     * @throws IOException if connection fails
     * @throws TimeoutException if connection times out
     */
    public RabbitMQBrokerConnector(RabbitMQConfig config) throws IOException, TimeoutException {
        this.config = Objects.requireNonNull(config, "config");
        this.connection = config.connectionFactory().newConnection();
        this.channel = connection.createChannel();
        LOG.info("Connected to RabbitMQ, exchange: {}", config.exchange());
    }

    @Override
    public PublishResult publish(OutboxEvent event) {
        Objects.requireNonNull(event, "event");

        String routingKey = buildRoutingKey(event);

        try {
            AMQP.BasicProperties properties = buildProperties(event);

            channel.basicPublish(
                config.exchange(),
                routingKey,
                config.mandatory(),
                properties,
                event.payload()
            );

            LOG.debug("Published event {} to exchange '{}' with routing key '{}'",
                event.id(), config.exchange(), routingKey);

            return PublishResult.success(event.id());
        } catch (IOException e) {
            LOG.error("Failed to publish event {} to RabbitMQ", event.id(), e);
            return PublishResult.failure(event.id(), e);
        }
    }

    @Override
    public boolean isHealthy() {
        return connection != null && connection.isOpen() && channel != null && channel.isOpen();
    }

    private String buildRoutingKey(OutboxEvent event) {
        return config.routingKeyPrefix() + event.aggregateType() + "." + event.eventType().value();
    }

    private AMQP.BasicProperties buildProperties(OutboxEvent event) {
        Map<String, Object> headers = new HashMap<>();
        headers.put("eventId", event.id().value().toString());
        headers.put("aggregateType", event.aggregateType());
        headers.put("aggregateId", event.aggregateId().value());
        headers.put("eventType", event.eventType().value());
        headers.put("createdAt", event.createdAt().toString());
        event.headers().forEach(headers::put);

        AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder()
            .contentType("application/octet-stream")
            .headers(headers)
            .messageId(event.id().value().toString())
            .timestamp(java.util.Date.from(event.createdAt()));

        if (config.persistent()) {
            builder.deliveryMode(2); // Persistent
        }

        return builder.build();
    }

    @Override
    public void close() {
        try {
            if (channel != null && channel.isOpen()) {
                channel.close();
            }
            if (connection != null && connection.isOpen()) {
                connection.close();
            }
            LOG.info("Closed RabbitMQ connection");
        } catch (IOException | TimeoutException e) {
            LOG.warn("Error closing RabbitMQ connection", e);
        }
    }
}
