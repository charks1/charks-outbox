package xyz.charks.outbox.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import xyz.charks.outbox.broker.PublishResult;
import xyz.charks.outbox.core.AggregateId;
import xyz.charks.outbox.core.EventType;
import xyz.charks.outbox.core.OutboxEvent;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration tests for RabbitMQBrokerConnector using Testcontainers with RabbitMQ.
 */
@Testcontainers
@DisplayName("RabbitMQBrokerConnector Integration Tests")
class RabbitMQBrokerConnectorIT {

    @Container
    static RabbitMQContainer rabbitmq = new RabbitMQContainer("rabbitmq:3.12-management");

    private static final String EXCHANGE = "test-outbox-exchange";
    private static final String QUEUE = "test-outbox-queue";

    private RabbitMQBrokerConnector connector;
    private Connection consumerConnection;
    private Channel consumerChannel;

    @BeforeEach
    void setUp() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(rabbitmq.getHost());
        factory.setPort(rabbitmq.getAmqpPort());
        factory.setUsername(rabbitmq.getAdminUsername());
        factory.setPassword(rabbitmq.getAdminPassword());

        consumerConnection = factory.newConnection();
        consumerChannel = consumerConnection.createChannel();
        consumerChannel.exchangeDeclare(EXCHANGE, "topic", true);
        consumerChannel.queueDeclare(QUEUE, true, false, false, null);
        consumerChannel.queueBind(QUEUE, EXCHANGE, "outbox.#");

        RabbitMQConfig config = RabbitMQConfig.builder()
                .connectionFactory(factory)
                .exchange(EXCHANGE)
                .routingKeyPrefix("outbox.")
                .persistent(true)
                .build();
        connector = new RabbitMQBrokerConnector(config);
    }

    @AfterEach
    void tearDown() throws IOException, TimeoutException {
        if (connector != null) {
            connector.close();
        }
        if (consumerChannel != null && consumerChannel.isOpen()) {
            consumerChannel.queueDelete(QUEUE);
            consumerChannel.exchangeDelete(EXCHANGE);
            consumerChannel.close();
        }
        if (consumerConnection != null && consumerConnection.isOpen()) {
            consumerConnection.close();
        }
    }

    @Nested
    @DisplayName("publish")
    class PublishIT {

        @Test
        @DisplayName("publishes event to RabbitMQ exchange and receives it")
        void publishesEventToRabbitMQ() {
            OutboxEvent event = createTestEvent();

            PublishResult result = connector.publish(event);

            assertThat(result.success()).isTrue();
            assertThat(result.eventId()).isEqualTo(event.id());

            AtomicReference<GetResponse> receivedMessage = new AtomicReference<>();
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                GetResponse response = consumerChannel.basicGet(QUEUE, true);
                receivedMessage.set(response);
                assertThat(response).isNotNull();
            });

            GetResponse response = receivedMessage.get();
            assertThat(response.getBody()).isEqualTo(event.payload());
        }

        @Test
        @DisplayName("includes event metadata in message headers")
        void includesEventMetadataInHeaders() {
            OutboxEvent event = OutboxEvent.builder()
                    .aggregateType("Order")
                    .aggregateId(AggregateId.of("order-123"))
                    .eventType(EventType.of("OrderCreated"))
                    .topic("orders")
                    .payload("{\"amount\":100}".getBytes(StandardCharsets.UTF_8))
                    .headers(Map.of("traceId", "trace-abc"))
                    .build();

            connector.publish(event);

            AtomicReference<GetResponse> receivedMessage = new AtomicReference<>();
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                GetResponse response = consumerChannel.basicGet(QUEUE, true);
                receivedMessage.set(response);
                assertThat(response).isNotNull();
            });

            AMQP.BasicProperties props = receivedMessage.get().getProps();
            Map<String, Object> headers = props.getHeaders();

            assertThat(headers.get("eventId")).hasToString(event.id().value().toString());
            assertThat(headers.get("aggregateType")).hasToString("Order");
            assertThat(headers.get("aggregateId")).hasToString("order-123");
            assertThat(headers.get("eventType")).hasToString("OrderCreated");
            assertThat(headers.get("createdAt")).hasToString(event.createdAt().toString());
            assertThat(headers.get("traceId")).hasToString("trace-abc");
            assertThat(props.getMessageId()).isEqualTo(event.id().value().toString());
        }

        @Test
        @DisplayName("uses correct routing key based on aggregate type and event type")
        void usesCorrectRoutingKey() throws IOException {
            String specificQueue = "specific-queue-" + UUID.randomUUID();
            consumerChannel.queueDeclare(specificQueue, false, false, true, null);
            consumerChannel.queueBind(specificQueue, EXCHANGE, "outbox.Payment.PaymentReceived");

            OutboxEvent event = OutboxEvent.builder()
                    .aggregateType("Payment")
                    .aggregateId(AggregateId.of("pay-123"))
                    .eventType(EventType.of("PaymentReceived"))
                    .topic("payments")
                    .payload("{}".getBytes(StandardCharsets.UTF_8))
                    .build();

            connector.publish(event);

            AtomicReference<GetResponse> receivedMessage = new AtomicReference<>();
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                GetResponse response = consumerChannel.basicGet(specificQueue, true);
                receivedMessage.set(response);
                assertThat(response).isNotNull();
            });

            assertThat(receivedMessage.get().getEnvelope().getRoutingKey())
                    .isEqualTo("outbox.Payment.PaymentReceived");
        }
    }

    @Nested
    @DisplayName("isHealthy")
    class IsHealthyIT {

        @Test
        @DisplayName("returns true when connected to RabbitMQ")
        void returnsTrueWhenConnected() {
            boolean healthy = connector.isHealthy();

            assertThat(healthy).isTrue();
        }

        @Test
        @DisplayName("returns false after connection is closed")
        void returnsFalseAfterClose() {
            connector.close();

            boolean healthy = connector.isHealthy();

            assertThat(healthy).isFalse();
            connector = null;
        }
    }

    private OutboxEvent createTestEvent() {
        return OutboxEvent.builder()
                .aggregateType("Order")
                .aggregateId(AggregateId.of("order-" + UUID.randomUUID()))
                .eventType(EventType.of("OrderCreated"))
                .topic("orders")
                .partitionKey("partition-1")
                .payload("{\"test\":true}".getBytes(StandardCharsets.UTF_8))
                .build();
    }
}
