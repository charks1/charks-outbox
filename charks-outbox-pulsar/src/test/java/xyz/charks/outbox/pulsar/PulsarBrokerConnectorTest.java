package xyz.charks.outbox.pulsar;

import org.apache.pulsar.client.api.PulsarClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import xyz.charks.outbox.core.AggregateId;
import xyz.charks.outbox.core.EventType;
import xyz.charks.outbox.core.OutboxEvent;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@DisplayName("PulsarBrokerConnector")
class PulsarBrokerConnectorTest {

    private PulsarClient pulsarClient;
    private PulsarBrokerConnector connector;

    @BeforeEach
    void setUp() {
        pulsarClient = mock(PulsarClient.class);
        when(pulsarClient.isClosed()).thenReturn(false);

        PulsarConfig config = PulsarConfig.builder()
                .client(pulsarClient)
                .topicPrefix("persistent://test/ns/outbox-")
                .build();

        connector = new PulsarBrokerConnector(config);
    }

    @Nested
    @DisplayName("constructor")
    class ConstructorTest {

        @Test
        @DisplayName("throws exception for null config")
        void nullConfig() {
            assertThatThrownBy(() -> new PulsarBrokerConnector(null))
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
    }

    @Nested
    @DisplayName("isHealthy")
    class IsHealthyTest {

        @Test
        @DisplayName("returns true when client is not closed")
        void healthyWhenOpen() {
            when(pulsarClient.isClosed()).thenReturn(false);

            assertThat(connector.isHealthy()).isTrue();
        }

        @Test
        @DisplayName("returns false when client is closed")
        void unhealthyWhenClosed() {
            when(pulsarClient.isClosed()).thenReturn(true);

            assertThat(connector.isHealthy()).isFalse();
        }
    }

    @Nested
    @DisplayName("close")
    class CloseTest {

        @Test
        @DisplayName("closes without error when no producers exist")
        void closesWithoutProducers() {
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
