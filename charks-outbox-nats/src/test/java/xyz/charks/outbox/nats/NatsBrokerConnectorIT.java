package xyz.charks.outbox.nats;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Nats;
import io.nats.client.Options;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import xyz.charks.outbox.broker.PublishResult;
import xyz.charks.outbox.core.AggregateId;
import xyz.charks.outbox.core.EventType;
import xyz.charks.outbox.core.OutboxEvent;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for NatsBrokerConnector using Testcontainers with NATS.
 */
@Testcontainers
@DisplayName("NatsBrokerConnector Integration Tests")
class NatsBrokerConnectorIT {

    private static final int NATS_PORT = 4222;

    @Container
    static GenericContainer<?> nats = new GenericContainer<>(DockerImageName.parse("nats:2.10"))
            .withExposedPorts(NATS_PORT);

    private static final String SUBJECT_PREFIX = "test.outbox.";

    private Connection natsConnection;
    private NatsBrokerConnector connector;

    @BeforeEach
    void setUp() throws Exception {
        String natsUrl = "nats://" + nats.getHost() + ":" + nats.getMappedPort(NATS_PORT);

        Options options = new Options.Builder()
                .server(natsUrl)
                .build();
        natsConnection = Nats.connect(options);

        NatsConfig config = NatsConfig.builder()
                .connection(natsConnection)
                .subjectPrefix(SUBJECT_PREFIX)
                .build();
        connector = new NatsBrokerConnector(config);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (connector != null) {
            connector.close();
        }
    }

    @Nested
    @DisplayName("publish")
    class PublishIT {

        @Test
        @DisplayName("publishes event to NATS subject and receives it")
        void publishesEventToNats() throws Exception {
            String aggregateType = "order";
            String eventType = "OrderCreated";
            String subject = SUBJECT_PREFIX + aggregateType + "." + eventType;

            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<io.nats.client.Message> receivedMessage = new AtomicReference<>();

            Connection subConnection = Nats.connect("nats://" + nats.getHost() + ":" + nats.getMappedPort(NATS_PORT));
            Dispatcher dispatcher = subConnection.createDispatcher(msg -> {
                receivedMessage.set(msg);
                latch.countDown();
            });
            dispatcher.subscribe(subject);

            OutboxEvent event = OutboxEvent.builder()
                    .aggregateType(aggregateType)
                    .aggregateId(AggregateId.of("order-123"))
                    .eventType(EventType.of(eventType))
                    .topic("orders")
                    .payload("{\"test\":true}".getBytes(StandardCharsets.UTF_8))
                    .build();

            PublishResult result = connector.publish(event);

            assertThat(result.success()).isTrue();
            assertThat(result.eventId()).isEqualTo(event.id());

            boolean received = latch.await(10, TimeUnit.SECONDS);
            assertThat(received).isTrue();

            io.nats.client.Message message = receivedMessage.get();
            assertThat(message.getData()).isEqualTo(event.payload());
            assertThat(message.getSubject()).isEqualTo(subject);

            subConnection.close();
        }

        @Test
        @DisplayName("includes event metadata in NATS headers")
        void includesEventMetadataInHeaders() throws Exception {
            String aggregateType = "payment";
            String eventType = "PaymentReceived";
            String subject = SUBJECT_PREFIX + aggregateType + "." + eventType;

            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<io.nats.client.Message> receivedMessage = new AtomicReference<>();

            Connection subConnection = Nats.connect("nats://" + nats.getHost() + ":" + nats.getMappedPort(NATS_PORT));
            Dispatcher dispatcher = subConnection.createDispatcher(msg -> {
                receivedMessage.set(msg);
                latch.countDown();
            });
            dispatcher.subscribe(subject);

            OutboxEvent event = OutboxEvent.builder()
                    .aggregateType(aggregateType)
                    .aggregateId(AggregateId.of("pay-456"))
                    .eventType(EventType.of(eventType))
                    .topic("payments")
                    .payload("{\"amount\":100}".getBytes(StandardCharsets.UTF_8))
                    .headers(Map.of("traceId", "trace-abc", "correlationId", "corr-xyz"))
                    .build();

            connector.publish(event);

            boolean received = latch.await(10, TimeUnit.SECONDS);
            assertThat(received).isTrue();

            io.nats.client.Message message = receivedMessage.get();
            assertThat(message.getHeaders()).isNotNull();
            assertThat(message.getHeaders().getFirst("eventId")).isEqualTo(event.id().value().toString());
            assertThat(message.getHeaders().getFirst("aggregateType")).isEqualTo(aggregateType);
            assertThat(message.getHeaders().getFirst("aggregateId")).isEqualTo("pay-456");
            assertThat(message.getHeaders().getFirst("eventType")).isEqualTo(eventType);
            assertThat(message.getHeaders().getFirst("createdAt")).isEqualTo(event.createdAt().toString());
            assertThat(message.getHeaders().getFirst("traceId")).isEqualTo("trace-abc");
            assertThat(message.getHeaders().getFirst("correlationId")).isEqualTo("corr-xyz");

            subConnection.close();
        }

        @Test
        @DisplayName("handles wildcard subscriptions correctly")
        void handlesWildcardSubscriptions() throws Exception {
            CountDownLatch latch = new CountDownLatch(2);
            AtomicReference<Integer> receivedCount = new AtomicReference<>(0);

            Connection subConnection = Nats.connect("nats://" + nats.getHost() + ":" + nats.getMappedPort(NATS_PORT));
            Dispatcher dispatcher = subConnection.createDispatcher(msg -> {
                receivedCount.updateAndGet(c -> c + 1);
                latch.countDown();
            });
            dispatcher.subscribe(SUBJECT_PREFIX + ">");

            OutboxEvent event1 = OutboxEvent.builder()
                    .aggregateType("order")
                    .aggregateId(AggregateId.of("o1"))
                    .eventType(EventType.of("Created"))
                    .topic("orders")
                    .payload("{}".getBytes())
                    .build();

            OutboxEvent event2 = OutboxEvent.builder()
                    .aggregateType("payment")
                    .aggregateId(AggregateId.of("p1"))
                    .eventType(EventType.of("Received"))
                    .topic("payments")
                    .payload("{}".getBytes())
                    .build();

            connector.publish(event1);
            connector.publish(event2);

            boolean received = latch.await(10, TimeUnit.SECONDS);
            assertThat(received).isTrue();
            assertThat(receivedCount.get()).isEqualTo(2);

            subConnection.close();
        }
    }

    @Nested
    @DisplayName("isHealthy")
    class IsHealthyIT {

        @Test
        @DisplayName("returns true when connected to NATS")
        void returnsTrueWhenConnected() {
            boolean healthy = connector.isHealthy();

            assertThat(healthy).isTrue();
        }

        @Test
        @DisplayName("returns false after connection is closed")
        void returnsFalseAfterClose() throws Exception {
            connector.close();

            boolean healthy = connector.isHealthy();

            assertThat(healthy).isFalse();
            connector = null;
        }
    }
}
