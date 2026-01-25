/**
 * Broker connectivity interfaces for message publishing.
 *
 * <p>This package defines the contract that all message broker implementations
 * must fulfill. The library provides implementations for:
 * <ul>
 *   <li>Apache Kafka</li>
 *   <li>RabbitMQ</li>
 *   <li>Apache Pulsar</li>
 *   <li>AWS SQS</li>
 *   <li>NATS</li>
 * </ul>
 *
 * @see xyz.charks.outbox.broker.BrokerConnector
 * @see xyz.charks.outbox.broker.PublishResult
 */
@NullMarked
package xyz.charks.outbox.broker;

import org.jspecify.annotations.NullMarked;
