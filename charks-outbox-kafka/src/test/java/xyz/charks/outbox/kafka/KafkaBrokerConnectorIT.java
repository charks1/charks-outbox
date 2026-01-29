package xyz.charks.outbox.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import xyz.charks.outbox.broker.PublishResult;
import xyz.charks.outbox.core.AggregateId;
import xyz.charks.outbox.core.EventType;
import xyz.charks.outbox.core.OutboxEvent;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration tests for KafkaBrokerConnector using Testcontainers with Kafka.
 */
@Testcontainers
@DisplayName("KafkaBrokerConnector Integration Tests")
class KafkaBrokerConnectorIT {

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"));

    private KafkaBrokerConnector connector;
    private KafkaConsumer<String, byte[]> consumer;

    @BeforeEach
    void setUp() {
        KafkaConfig config = KafkaConfig.builder()
                .bootstrapServers(kafka.getBootstrapServers())
                .acks(KafkaConfig.Acks.ALL)
                .requestTimeout(Duration.ofSeconds(30))
                .build();
        connector = new KafkaBrokerConnector(config);

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumer = new KafkaConsumer<>(consumerProps);
    }

    @AfterEach
    void tearDown() {
        if (connector != null) {
            connector.close();
        }
        if (consumer != null) {
            consumer.close();
        }
    }

    @Nested
    @DisplayName("publish")
    class PublishIT {

        @Test
        @DisplayName("publishes event to Kafka topic and receives it")
        void publishesEventToKafka() {
            String topic = "test-orders-" + UUID.randomUUID();
            OutboxEvent event = createTestEvent(topic);

            consumer.subscribe(List.of(topic));

            PublishResult result = connector.publish(event);

            assertThat(result.success()).isTrue();
            assertThat(result.eventId()).isEqualTo(event.id());
            assertThat(result.partition()).isNotNull();
            assertThat(result.offset()).isNotNull();

            AtomicReference<ConsumerRecord<String, byte[]>> receivedRecord = new AtomicReference<>();
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, byte[]> record : records) {
                    if (record.topic().equals(topic)) {
                        receivedRecord.set(record);
                    }
                }
                assertThat(receivedRecord.get()).isNotNull();
            });

            ConsumerRecord<String, byte[]> record = receivedRecord.get();
            assertThat(record.key()).isEqualTo(event.partitionKey());
            assertThat(record.value()).isEqualTo(event.payload());
        }

        @Test
        @DisplayName("includes outbox headers in Kafka message")
        void includesOutboxHeaders() {
            String topic = "test-headers-" + UUID.randomUUID();
            OutboxEvent event = OutboxEvent.builder()
                    .aggregateType("Order")
                    .aggregateId(AggregateId.of("order-123"))
                    .eventType(EventType.of("OrderCreated"))
                    .topic(topic)
                    .partitionKey("key-1")
                    .payload("{\"test\":true}".getBytes(StandardCharsets.UTF_8))
                    .headers(Map.of("traceId", "trace-abc", "correlationId", "corr-xyz"))
                    .build();

            consumer.subscribe(List.of(topic));

            connector.publish(event);

            AtomicReference<ConsumerRecord<String, byte[]>> receivedRecord = new AtomicReference<>();
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, byte[]> record : records) {
                    if (record.topic().equals(topic)) {
                        receivedRecord.set(record);
                    }
                }
                assertThat(receivedRecord.get()).isNotNull();
            });

            ConsumerRecord<String, byte[]> record = receivedRecord.get();
            assertThat(getHeader(record, "outbox-event-id")).isEqualTo(event.id().value().toString());
            assertThat(getHeader(record, "outbox-event-type")).isEqualTo("OrderCreated");
            assertThat(getHeader(record, "outbox-aggregate-type")).isEqualTo("Order");
            assertThat(getHeader(record, "outbox-aggregate-id")).isEqualTo("order-123");
            assertThat(getHeader(record, "outbox-created-at")).isEqualTo(event.createdAt().toString());
            assertThat(getHeader(record, "traceId")).isEqualTo("trace-abc");
            assertThat(getHeader(record, "correlationId")).isEqualTo("corr-xyz");
        }
    }

    @Nested
    @DisplayName("publishAll")
    class PublishAllIT {

        @Test
        @DisplayName("publishes multiple events in batch")
        void publishesMultipleEvents() {
            String topic = "test-batch-" + UUID.randomUUID();
            List<OutboxEvent> events = List.of(
                    createTestEvent(topic),
                    createTestEvent(topic),
                    createTestEvent(topic)
            );

            consumer.subscribe(List.of(topic));

            List<PublishResult> results = connector.publishAll(events);

            assertThat(results).hasSize(3);
            assertThat(results).allMatch(PublishResult::success);

            AtomicReference<Integer> receivedCount = new AtomicReference<>(0);
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));
                receivedCount.updateAndGet(count -> count + records.count());
                assertThat(receivedCount.get()).isGreaterThanOrEqualTo(3);
            });
        }

        @Test
        @DisplayName("returns empty list for empty input")
        void returnsEmptyForEmptyInput() {
            List<PublishResult> results = connector.publishAll(List.of());

            assertThat(results).isEmpty();
        }
    }

    @Nested
    @DisplayName("isHealthy")
    class IsHealthyIT {

        @Test
        @DisplayName("returns true when connected to Kafka")
        void returnsTrueWhenConnected() {
            boolean healthy = connector.isHealthy();

            assertThat(healthy).isTrue();
        }
    }

    private OutboxEvent createTestEvent(String topic) {
        return OutboxEvent.builder()
                .aggregateType("Order")
                .aggregateId(AggregateId.of("order-" + UUID.randomUUID()))
                .eventType(EventType.of("OrderCreated"))
                .topic(topic)
                .partitionKey("partition-1")
                .payload("{\"test\":true}".getBytes(StandardCharsets.UTF_8))
                .build();
    }

    private String getHeader(ConsumerRecord<String, byte[]> record, String key) {
        var header = record.headers().lastHeader(key);
        return header != null ? new String(header.value(), StandardCharsets.UTF_8) : null;
    }
}
