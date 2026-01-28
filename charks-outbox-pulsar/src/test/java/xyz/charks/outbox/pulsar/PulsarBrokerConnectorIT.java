package xyz.charks.outbox.pulsar;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import xyz.charks.outbox.broker.PublishResult;
import xyz.charks.outbox.core.AggregateId;
import xyz.charks.outbox.core.EventType;
import xyz.charks.outbox.core.OutboxEvent;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration tests for PulsarBrokerConnector using Testcontainers with Apache Pulsar.
 */
@Testcontainers
@DisplayName("PulsarBrokerConnector Integration Tests")
class PulsarBrokerConnectorIT {

    @Container
    static PulsarContainer pulsar = new PulsarContainer(DockerImageName.parse("apachepulsar/pulsar:3.1.0"));

    private static final String TOPIC_PREFIX = "persistent://public/default/test-";

    private PulsarClient pulsarClient;
    private PulsarBrokerConnector connector;

    @BeforeEach
    void setUp() throws PulsarClientException {
        pulsarClient = PulsarClient.builder()
                .serviceUrl(pulsar.getPulsarBrokerUrl())
                .build();

        PulsarConfig config = PulsarConfig.builder()
                .client(pulsarClient)
                .topicPrefix(TOPIC_PREFIX)
                .enableBatching(false)
                .build();
        connector = new PulsarBrokerConnector(config);
    }

    @AfterEach
    void tearDown() throws PulsarClientException {
        if (connector != null) {
            connector.close();
        }
        if (pulsarClient != null) {
            pulsarClient.close();
        }
    }

    @Nested
    @DisplayName("publish")
    class PublishIT {

        @Test
        @DisplayName("publishes event to Pulsar topic and receives it")
        void publishesEventToPulsar() throws PulsarClientException {
            String aggregateType = "order" + UUID.randomUUID().toString().substring(0, 8);
            String topic = TOPIC_PREFIX + aggregateType;

            Consumer<byte[]> consumer = pulsarClient.newConsumer(Schema.BYTES)
                    .topic(topic)
                    .subscriptionName("test-sub-" + UUID.randomUUID())
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .subscribe();

            OutboxEvent event = OutboxEvent.builder()
                    .aggregateType(aggregateType)
                    .aggregateId(AggregateId.of("order-123"))
                    .eventType(EventType.of("OrderCreated"))
                    .topic("orders")
                    .payload("{\"test\":true}".getBytes(StandardCharsets.UTF_8))
                    .build();

            PublishResult result = connector.publish(event);

            assertThat(result.success()).isTrue();
            assertThat(result.eventId()).isEqualTo(event.id());

            AtomicReference<Message<byte[]>> receivedMessage = new AtomicReference<>();
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                Message<byte[]> msg = consumer.receive(1, TimeUnit.SECONDS);
                if (msg != null) {
                    receivedMessage.set(msg);
                    consumer.acknowledge(msg);
                }
                assertThat(receivedMessage.get()).isNotNull();
            });

            Message<byte[]> message = receivedMessage.get();
            assertThat(message.getData()).isEqualTo(event.payload());
            assertThat(message.getKey()).isEqualTo("order-123");

            consumer.close();
        }

        @Test
        @DisplayName("includes event metadata as message properties")
        void includesEventMetadataAsProperties() throws PulsarClientException {
            String aggregateType = "payment" + UUID.randomUUID().toString().substring(0, 8);
            String topic = TOPIC_PREFIX + aggregateType;

            Consumer<byte[]> consumer = pulsarClient.newConsumer(Schema.BYTES)
                    .topic(topic)
                    .subscriptionName("test-sub-" + UUID.randomUUID())
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .subscribe();

            OutboxEvent event = OutboxEvent.builder()
                    .aggregateType(aggregateType)
                    .aggregateId(AggregateId.of("pay-456"))
                    .eventType(EventType.of("PaymentReceived"))
                    .topic("payments")
                    .payload("{\"amount\":100}".getBytes(StandardCharsets.UTF_8))
                    .headers(Map.of("traceId", "trace-abc", "correlationId", "corr-xyz"))
                    .build();

            connector.publish(event);

            AtomicReference<Message<byte[]>> receivedMessage = new AtomicReference<>();
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                Message<byte[]> msg = consumer.receive(1, TimeUnit.SECONDS);
                if (msg != null) {
                    receivedMessage.set(msg);
                    consumer.acknowledge(msg);
                }
                assertThat(receivedMessage.get()).isNotNull();
            });

            Message<byte[]> message = receivedMessage.get();
            assertThat(message.getProperty("eventId")).isEqualTo(event.id().value().toString());
            assertThat(message.getProperty("aggregateType")).isEqualTo(aggregateType);
            assertThat(message.getProperty("aggregateId")).isEqualTo("pay-456");
            assertThat(message.getProperty("eventType")).isEqualTo("PaymentReceived");
            assertThat(message.getProperty("traceId")).isEqualTo("trace-abc");
            assertThat(message.getProperty("correlationId")).isEqualTo("corr-xyz");

            consumer.close();
        }
    }

    @Nested
    @DisplayName("isHealthy")
    class IsHealthyIT {

        @Test
        @DisplayName("returns true when client is connected")
        void returnsTrueWhenConnected() {
            boolean healthy = connector.isHealthy();

            assertThat(healthy).isTrue();
        }

        @Test
        @DisplayName("returns false after client is closed")
        void returnsFalseAfterClose() throws PulsarClientException {
            connector.close();
            pulsarClient.close();

            boolean healthy = connector.isHealthy();

            assertThat(healthy).isFalse();
            connector = null;
            pulsarClient = null;
        }
    }
}
