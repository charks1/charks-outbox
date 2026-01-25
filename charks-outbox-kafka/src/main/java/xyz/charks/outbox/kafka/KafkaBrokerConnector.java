package xyz.charks.outbox.kafka;

import xyz.charks.outbox.broker.BrokerConnector;
import xyz.charks.outbox.broker.PublishResult;
import xyz.charks.outbox.core.OutboxEvent;
import xyz.charks.outbox.exception.OutboxPublishException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Kafka implementation of {@link BrokerConnector}.
 *
 * <p>Publishes outbox events to Apache Kafka topics using the Kafka producer API.
 * Supports:
 * <ul>
 *   <li>Topic routing based on event's topic field</li>
 *   <li>Partition key routing for ordered delivery</li>
 *   <li>Header propagation</li>
 *   <li>Batch publishing for improved throughput</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * KafkaConfig config = KafkaConfig.builder()
 *     .bootstrapServers("localhost:9092")
 *     .acks(Acks.ALL)
 *     .build();
 *
 * try (KafkaBrokerConnector connector = new KafkaBrokerConnector(config)) {
 *     PublishResult result = connector.publish(event);
 *     if (result.success()) {
 *         log.info("Published to partition {} at offset {}",
 *             result.partition(), result.offset());
 *     }
 * }
 * }</pre>
 *
 * @see KafkaConfig
 */
public class KafkaBrokerConnector implements BrokerConnector {

    private static final Logger log = LoggerFactory.getLogger(KafkaBrokerConnector.class);

    private final KafkaProducer<String, byte[]> producer;
    private final Duration sendTimeout;

    /**
     * Creates a new Kafka connector with the specified configuration.
     *
     * @param config the Kafka configuration
     * @throws NullPointerException if config is null
     */
    public KafkaBrokerConnector(KafkaConfig config) {
        Objects.requireNonNull(config, "Config cannot be null");
        this.producer = new KafkaProducer<>(config.toProducerProperties());
        this.sendTimeout = config.requestTimeout();
        log.info("Kafka connector initialized with bootstrap servers: {}",
                config.bootstrapServers());
    }

    /**
     * Creates a new Kafka connector with an existing producer.
     *
     * <p>Useful for testing or when sharing a producer instance.
     *
     * @param producer the Kafka producer to use
     * @param sendTimeout timeout for send operations
     * @throws NullPointerException if producer or sendTimeout is null
     */
    public KafkaBrokerConnector(KafkaProducer<String, byte[]> producer, Duration sendTimeout) {
        this.producer = Objects.requireNonNull(producer, "Producer cannot be null");
        this.sendTimeout = Objects.requireNonNull(sendTimeout, "Send timeout cannot be null");
    }

    @Override
    public PublishResult publish(OutboxEvent event) {
        Objects.requireNonNull(event, "Event cannot be null");

        ProducerRecord<String, byte[]> record = createRecord(event);

        try {
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS);

            log.debug("Published event {} to topic {} partition {} offset {}",
                    event.id(), metadata.topic(), metadata.partition(), metadata.offset());

            return PublishResult.success(
                    event.id(),
                    null,
                    metadata.partition(),
                    metadata.offset()
            );
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            log.error("Failed to publish event {} to topic {}: {}",
                    event.id(), event.topic(), cause.getMessage());
            return PublishResult.failure(event.id(), cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while publishing event {} to topic {}",
                    event.id(), event.topic());
            return PublishResult.failure(event.id(), "Interrupted during publish");
        } catch (TimeoutException e) {
            log.error("Timeout publishing event {} to topic {} after {}",
                    event.id(), event.topic(), sendTimeout);
            return PublishResult.failure(event.id(), "Publish timeout after " + sendTimeout);
        }
    }

    @Override
    public List<PublishResult> publishAll(List<OutboxEvent> events) {
        Objects.requireNonNull(events, "Events cannot be null");

        if (events.isEmpty()) {
            return List.of();
        }

        List<Future<RecordMetadata>> futures = new ArrayList<>(events.size());
        for (OutboxEvent event : events) {
            ProducerRecord<String, byte[]> record = createRecord(event);
            futures.add(producer.send(record));
        }

        producer.flush();

        List<PublishResult> results = new ArrayList<>(events.size());
        for (int i = 0; i < events.size(); i++) {
            OutboxEvent event = events.get(i);
            Future<RecordMetadata> future = futures.get(i);

            try {
                RecordMetadata metadata = future.get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS);
                results.add(PublishResult.success(
                        event.id(),
                        null,
                        metadata.partition(),
                        metadata.offset()
                ));
            } catch (ExecutionException e) {
                Throwable cause = e.getCause() != null ? e.getCause() : e;
                results.add(PublishResult.failure(event.id(), cause));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                results.add(PublishResult.failure(event.id(), "Interrupted during publish"));
            } catch (TimeoutException e) {
                results.add(PublishResult.failure(event.id(), "Publish timeout after " + sendTimeout));
            }
        }

        return results;
    }

    @Override
    public boolean isHealthy() {
        try {
            producer.partitionsFor("__health_check");
            return true;
        } catch (Exception e) {
            log.warn("Kafka health check failed: {}", e.getMessage());
            return false;
        }
    }

    @Override
    public void close() {
        log.info("Closing Kafka connector");
        try {
            producer.flush();
            producer.close(Duration.ofSeconds(30));
            log.info("Kafka connector closed");
        } catch (Exception e) {
            log.error("Error closing Kafka producer: {}", e.getMessage());
            throw new OutboxPublishException("Failed to close Kafka producer", e);
        }
    }

    private ProducerRecord<String, byte[]> createRecord(OutboxEvent event) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                event.topic(),
                event.partitionKey(),
                event.payload()
        );

        event.headers().forEach((key, value) ->
                record.headers().add(new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8)))
        );

        record.headers().add(new RecordHeader("outbox-event-id",
                event.id().value().toString().getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader("outbox-event-type",
                event.eventType().value().getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader("outbox-aggregate-type",
                event.aggregateType().getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader("outbox-aggregate-id",
                event.aggregateId().value().getBytes(StandardCharsets.UTF_8)));

        return record;
    }
}
