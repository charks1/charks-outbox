package xyz.charks.outbox.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import xyz.charks.outbox.broker.PublishResult;
import xyz.charks.outbox.core.AggregateId;
import xyz.charks.outbox.core.EventType;
import xyz.charks.outbox.core.OutboxEvent;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@DisplayName("RabbitMQBrokerConnector")
class RabbitMQBrokerConnectorTest {

    private ConnectionFactory connectionFactory;
    private Connection connection;
    private Channel channel;
    private RabbitMQBrokerConnector connector;

    @BeforeEach
    void setUp() throws IOException, TimeoutException {
        connectionFactory = mock(ConnectionFactory.class);
        connection = mock(Connection.class);
        channel = mock(Channel.class);

        when(connectionFactory.newConnection()).thenReturn(connection);
        when(connection.createChannel()).thenReturn(channel);
        when(connection.isOpen()).thenReturn(true);
        when(channel.isOpen()).thenReturn(true);

        RabbitMQConfig config = RabbitMQConfig.builder()
                .connectionFactory(connectionFactory)
                .exchange("test-exchange")
                .routingKeyPrefix("test.")
                .build();

        connector = new RabbitMQBrokerConnector(config);
    }

    @Nested
    @DisplayName("constructor")
    class ConstructorTest {

        @Test
        @DisplayName("throws exception for null config")
        void nullConfig() {
            assertThatThrownBy(() -> new RabbitMQBrokerConnector(null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("config");
        }

        @Test
        @DisplayName("creates connection and channel")
        void createsConnectionAndChannel() throws IOException, TimeoutException {
            verify(connectionFactory).newConnection();
            verify(connection).createChannel();
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
        @DisplayName("publishes event to exchange with routing key")
        void publishesEvent() throws IOException {
            OutboxEvent event = createTestEvent();

            PublishResult result = connector.publish(event);

            assertThat(result.success()).isTrue();
            assertThat(result.eventId()).isEqualTo(event.id());

            verify(channel).basicPublish(
                    eq("test-exchange"),
                    eq("test.Order.OrderCreated"),
                    anyBoolean(),
                    any(),
                    eq(event.payload())
            );
        }

        @Test
        @DisplayName("returns failure when publish throws exception")
        void publishFailure() throws IOException {
            OutboxEvent event = createTestEvent();
            doThrow(new IOException("Connection lost")).when(channel)
                    .basicPublish(anyString(), anyString(), anyBoolean(), any(), any());

            PublishResult result = connector.publish(event);

            assertThat(result.success()).isFalse();
            assertThat(result.eventId()).isEqualTo(event.id());
            assertThat(result.getError()).isPresent();
        }
    }

    @Nested
    @DisplayName("isHealthy")
    class IsHealthyTest {

        @Test
        @DisplayName("returns true when connection and channel are open")
        void healthyWhenOpen() {
            when(connection.isOpen()).thenReturn(true);
            when(channel.isOpen()).thenReturn(true);

            assertThat(connector.isHealthy()).isTrue();
        }

        @Test
        @DisplayName("returns false when connection is closed")
        void unhealthyWhenConnectionClosed() {
            when(connection.isOpen()).thenReturn(false);

            assertThat(connector.isHealthy()).isFalse();
        }

        @Test
        @DisplayName("returns false when channel is closed")
        void unhealthyWhenChannelClosed() {
            when(connection.isOpen()).thenReturn(true);
            when(channel.isOpen()).thenReturn(false);

            assertThat(connector.isHealthy()).isFalse();
        }
    }

    @Nested
    @DisplayName("close")
    class CloseTest {

        @Test
        @DisplayName("closes channel and connection")
        void closesResources() throws IOException, TimeoutException {
            connector.close();

            verify(channel).close();
            verify(connection).close();
        }

        @Test
        @DisplayName("handles close exceptions gracefully")
        void handlesCloseException() throws IOException, TimeoutException {
            doThrow(new IOException("Close failed")).when(channel).close();

            // Should not throw
            connector.close();

            verify(channel).close();
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
