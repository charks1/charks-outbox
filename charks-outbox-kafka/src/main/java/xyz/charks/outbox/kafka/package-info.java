/**
 * Apache Kafka implementation of the Outbox broker connector.
 *
 * <p>This module provides a Kafka producer-based implementation of
 * {@link xyz.charks.outbox.broker.BrokerConnector} for publishing
 * outbox events to Kafka topics.
 *
 * <h2>Key Classes</h2>
 * <ul>
 *   <li>{@link xyz.charks.outbox.kafka.KafkaBrokerConnector} - The main connector implementation</li>
 *   <li>{@link xyz.charks.outbox.kafka.KafkaConfig} - Configuration for the Kafka producer</li>
 * </ul>
 *
 * <h2>Usage Example</h2>
 * <pre>{@code
 * KafkaConfig config = KafkaConfig.builder()
 *     .bootstrapServers("localhost:9092")
 *     .acks(Acks.ALL)
 *     .build();
 *
 * try (KafkaBrokerConnector connector = new KafkaBrokerConnector(config)) {
 *     PublishResult result = connector.publish(event);
 * }
 * }</pre>
 *
 * @see xyz.charks.outbox.kafka.KafkaBrokerConnector
 * @see xyz.charks.outbox.kafka.KafkaConfig
 */
@NullMarked
package xyz.charks.outbox.kafka;

import org.jspecify.annotations.NullMarked;
