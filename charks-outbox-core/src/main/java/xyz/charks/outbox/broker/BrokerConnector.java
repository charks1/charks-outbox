package xyz.charks.outbox.broker;

import xyz.charks.outbox.core.OutboxEvent;

import java.util.List;

/**
 * Interface for publishing outbox events to message brokers.
 *
 * <p>Implementations provide connectivity to specific messaging systems:
 * <ul>
 *   <li>Apache Kafka</li>
 *   <li>RabbitMQ</li>
 *   <li>Apache Pulsar</li>
 *   <li>AWS SQS</li>
 *   <li>NATS</li>
 * </ul>
 *
 * <p>The connector is responsible for:
 * <ul>
 *   <li>Establishing and managing broker connections</li>
 *   <li>Serializing message headers and metadata</li>
 *   <li>Publishing to the correct topic/queue</li>
 *   <li>Handling partition key routing</li>
 *   <li>Reporting publish results</li>
 * </ul>
 *
 * <p>Example implementation for Kafka:
 * <pre>{@code
 * public class KafkaBrokerConnector implements BrokerConnector {
 *     private final KafkaProducer<String, byte[]> producer;
 *
 *     public PublishResult publish(OutboxEvent event) {
 *         ProducerRecord<String, byte[]> record = new ProducerRecord<>(
 *             event.topic(),
 *             event.partitionKey(),
 *             event.payload()
 *         );
 *         event.headers().forEach((k, v) ->
 *             record.headers().add(k, v.getBytes(UTF_8)));
 *
 *         RecordMetadata metadata = producer.send(record).get();
 *         return PublishResult.success(
 *             event.id(),
 *             null,
 *             metadata.partition(),
 *             metadata.offset()
 *         );
 *     }
 * }
 * }</pre>
 *
 * @see PublishResult
 */
public interface BrokerConnector {

    /**
     * Publishes a single outbox event to the message broker.
     *
     * <p>The event's topic determines the destination. If a partition key is
     * specified, it should be used for message routing when the broker supports it.
     *
     * @param event the event to publish
     * @return the result of the publish operation
     * @throws OutboxPublishException if publishing fails with an unrecoverable error
     * @throws NullPointerException if event is null
     */
    PublishResult publish(OutboxEvent event);

    /**
     * Publishes multiple outbox events to the message broker.
     *
     * <p>The default implementation publishes events sequentially. Implementations
     * may override this to use batch publishing for better performance.
     *
     * <p>All events should be attempted even if some fail. The returned list
     * contains results for all events in the same order as the input.
     *
     * @param events the events to publish
     * @return results for each event in input order
     * @throws NullPointerException if events is null
     */
    default List<PublishResult> publishAll(List<OutboxEvent> events) {
        return events.stream()
                .map(this::publish)
                .toList();
    }

    /**
     * Tests the connection to the message broker.
     *
     * <p>Use this method for health checks and startup validation.
     *
     * @return true if the broker is reachable and ready
     */
    default boolean isHealthy() {
        return true;
    }

    /**
     * Closes the connection to the message broker.
     *
     * <p>Implementations should flush any pending messages and release resources.
     * This method should be called during application shutdown.
     *
     * @throws OutboxPublishException if closing fails
     */
    default void close() {
        // Default no-op for stateless implementations
    }
}
