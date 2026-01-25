package xyz.charks.outbox.nats;

import io.nats.client.Connection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import xyz.charks.outbox.broker.PublishResult;
import xyz.charks.outbox.core.AggregateId;
import xyz.charks.outbox.core.EventType;
import xyz.charks.outbox.core.OutboxEvent;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@DisplayName("NatsBrokerConnector")
class NatsBrokerConnectorTest {

    private Connection natsConnection;
    private NatsBrokerConnector connector;

    @BeforeEach
    void setUp() {
        natsConnection = mock(Connection.class);
        when(natsConnection.getStatus()).thenReturn(Connection.Status.CONNECTED);

        NatsConfig config = NatsConfig.builder()
                .connection(natsConnection)
                .subjectPrefix("test.")
                .build();

        connector = new NatsBrokerConnector(config);
    }

    @Nested
    @DisplayName("constructor")
    class ConstructorTest {

        @Test
        @DisplayName("throws exception for null config")
        void nullConfig() {
            assertThatThrownBy(() -> new NatsBrokerConnector(null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("config");
        }
    }

    @Nested
    @DisplayName("publish")
    class PublishTest {

        @Test
        @DisplayName("throws exception for null event")
        void nullEvent() {
            assertThatThrownBy(() -> connector.publish(null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("event");
        }

        @Test
        @DisplayName("publishes event to NATS subject")
        void publishesEvent() {
            OutboxEvent event = createTestEvent();

            PublishResult result = connector.publish(event);

            assertThat(result.success()).isTrue();
            assertThat(result.eventId()).isEqualTo(event.id());
        }
    }

    @Nested
    @DisplayName("isHealthy")
    class IsHealthyTest {

        @Test
        @DisplayName("returns true when connection is connected")
        void healthyWhenConnected() {
            when(natsConnection.getStatus()).thenReturn(Connection.Status.CONNECTED);

            assertThat(connector.isHealthy()).isTrue();
        }

        @Test
        @DisplayName("returns false when connection is disconnected")
        void unhealthyWhenDisconnected() {
            when(natsConnection.getStatus()).thenReturn(Connection.Status.DISCONNECTED);

            assertThat(connector.isHealthy()).isFalse();
        }

        @Test
        @DisplayName("returns false when connection is closed")
        void unhealthyWhenClosed() {
            when(natsConnection.getStatus()).thenReturn(Connection.Status.CLOSED);

            assertThat(connector.isHealthy()).isFalse();
        }
    }

    @Nested
    @DisplayName("close")
    class CloseTest {

        @Test
        @DisplayName("closes connection when connected")
        void closesConnection() throws InterruptedException {
            when(natsConnection.getStatus()).thenReturn(Connection.Status.CONNECTED);

            // Should not throw
            connector.close();
        }

        @Test
        @DisplayName("skips close when already disconnected")
        void skipsCloseWhenDisconnected() {
            when(natsConnection.getStatus()).thenReturn(Connection.Status.DISCONNECTED);

            // Should not throw
            connector.close();
        }
    }

    private OutboxEvent createTestEvent() {
        return OutboxEvent.builder()
                .aggregateType("Order")
                .aggregateId(AggregateId.of("order-123"))
                .eventType(EventType.of("OrderCreated"))
                .topic("orders")
                .partitionKey("order-123")
                .payload("{\"test\":true}".getBytes(StandardCharsets.UTF_8))
                .build();
    }
}
