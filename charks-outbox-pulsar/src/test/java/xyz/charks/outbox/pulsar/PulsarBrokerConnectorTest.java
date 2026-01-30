package xyz.charks.outbox.pulsar;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import xyz.charks.outbox.broker.PublishResult;
import xyz.charks.outbox.core.AggregateId;
import xyz.charks.outbox.core.EventType;
import xyz.charks.outbox.core.OutboxEvent;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@Timeout(30)
@DisplayName("PulsarBrokerConnector")
class PulsarBrokerConnectorTest {

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

        @Test
        @DisplayName("creates connector with valid config")
        void validConfig() {
            PulsarClient client = mock(PulsarClient.class);
            PulsarConfig config = PulsarConfig.builder()
                    .client(client)
                    .topicPrefix("persistent://tenant/ns/")
                    .build();

            PulsarBrokerConnector conn = new PulsarBrokerConnector(config);

            assertThat(conn).isNotNull();
        }
    }

    @Nested
    @DisplayName("publish")
    class PublishTest {

        @Test
        @DisplayName("throws exception for null event")
        void nullEvent() {
            PulsarClient client = mock(PulsarClient.class);
            PulsarConfig config = PulsarConfig.builder()
                    .client(client)
                    .topicPrefix("test-")
                    .build();
            PulsarBrokerConnector connector = new PulsarBrokerConnector(config);

            assertThatThrownBy(() -> connector.publish(null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("event");
        }

        @Test
        @DisplayName("publishes event successfully")
        void publishSuccess() throws PulsarClientException {
            PulsarClient client = mock(PulsarClient.class);
            ProducerBuilder<byte[]> producerBuilder = mock(ProducerBuilder.class);
            Producer<byte[]> producer = mock(Producer.class);
            TypedMessageBuilder<byte[]> messageBuilder = mock(TypedMessageBuilder.class);

            when(client.newProducer()).thenReturn(producerBuilder);
            when(producerBuilder.topic(anyString())).thenReturn(producerBuilder);
            when(producerBuilder.enableBatching(anyBoolean())).thenReturn(producerBuilder);
            when(producerBuilder.batchingMaxMessages(anyInt())).thenReturn(producerBuilder);
            when(producerBuilder.create()).thenReturn(producer);
            when(producer.newMessage()).thenReturn(messageBuilder);
            when(messageBuilder.key(anyString())).thenReturn(messageBuilder);
            when(messageBuilder.value(any(byte[].class))).thenReturn(messageBuilder);
            when(messageBuilder.property(anyString(), anyString())).thenReturn(messageBuilder);
            when(messageBuilder.send()).thenReturn(MessageId.earliest);

            PulsarConfig config = PulsarConfig.builder()
                    .client(client)
                    .topicPrefix("persistent://test/ns/outbox-")
                    .build();
            PulsarBrokerConnector connector = new PulsarBrokerConnector(config);

            OutboxEvent event = createTestEvent();
            PublishResult result = connector.publish(event);

            assertThat(result.success()).isTrue();
            assertThat(result.eventId()).isEqualTo(event.id());
        }

        @Test
        @DisplayName("publishes event with headers")
        void publishWithHeaders() throws PulsarClientException {
            PulsarClient client = mock(PulsarClient.class);
            ProducerBuilder<byte[]> producerBuilder = mock(ProducerBuilder.class);
            Producer<byte[]> producer = mock(Producer.class);
            TypedMessageBuilder<byte[]> messageBuilder = mock(TypedMessageBuilder.class);

            when(client.newProducer()).thenReturn(producerBuilder);
            when(producerBuilder.topic(anyString())).thenReturn(producerBuilder);
            when(producerBuilder.enableBatching(anyBoolean())).thenReturn(producerBuilder);
            when(producerBuilder.batchingMaxMessages(anyInt())).thenReturn(producerBuilder);
            when(producerBuilder.create()).thenReturn(producer);
            when(producer.newMessage()).thenReturn(messageBuilder);
            when(messageBuilder.key(anyString())).thenReturn(messageBuilder);
            when(messageBuilder.value(any(byte[].class))).thenReturn(messageBuilder);
            when(messageBuilder.property(anyString(), anyString())).thenReturn(messageBuilder);
            when(messageBuilder.send()).thenReturn(MessageId.earliest);

            PulsarConfig config = PulsarConfig.builder()
                    .client(client)
                    .topicPrefix("persistent://test/ns/outbox-")
                    .build();
            PulsarBrokerConnector connector = new PulsarBrokerConnector(config);

            OutboxEvent event = OutboxEvent.builder()
                    .aggregateType("Order")
                    .aggregateId(AggregateId.of("order-123"))
                    .eventType(EventType.of("OrderCreated"))
                    .topic("orders")
                    .partitionKey("order-123")
                    .payload("{\"test\":true}".getBytes(StandardCharsets.UTF_8))
                    .headers(Map.of("correlationId", "corr-123", "version", "1"))
                    .build();

            PublishResult result = connector.publish(event);

            assertThat(result.success()).isTrue();
        }

        @Test
        @DisplayName("returns failure when send throws exception")
        void publishFailure() throws PulsarClientException {
            PulsarClient client = mock(PulsarClient.class);
            ProducerBuilder<byte[]> producerBuilder = mock(ProducerBuilder.class);
            Producer<byte[]> producer = mock(Producer.class);
            TypedMessageBuilder<byte[]> messageBuilder = mock(TypedMessageBuilder.class);

            when(client.newProducer()).thenReturn(producerBuilder);
            when(producerBuilder.topic(anyString())).thenReturn(producerBuilder);
            when(producerBuilder.enableBatching(anyBoolean())).thenReturn(producerBuilder);
            when(producerBuilder.batchingMaxMessages(anyInt())).thenReturn(producerBuilder);
            when(producerBuilder.create()).thenReturn(producer);
            when(producer.newMessage()).thenReturn(messageBuilder);
            when(messageBuilder.key(anyString())).thenReturn(messageBuilder);
            when(messageBuilder.value(any(byte[].class))).thenReturn(messageBuilder);
            when(messageBuilder.property(anyString(), anyString())).thenReturn(messageBuilder);
            when(messageBuilder.send()).thenThrow(new PulsarClientException("Connection failed"));

            PulsarConfig config = PulsarConfig.builder()
                    .client(client)
                    .topicPrefix("persistent://test/ns/outbox-")
                    .build();
            PulsarBrokerConnector connector = new PulsarBrokerConnector(config);

            OutboxEvent event = createTestEvent();
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
        @DisplayName("returns true when client is not closed")
        void healthyWhenOpen() {
            PulsarClient client = mock(PulsarClient.class);
            when(client.isClosed()).thenReturn(false);

            PulsarConfig config = PulsarConfig.builder()
                    .client(client)
                    .topicPrefix("test-")
                    .build();
            PulsarBrokerConnector connector = new PulsarBrokerConnector(config);

            assertThat(connector.isHealthy()).isTrue();
        }

        @Test
        @DisplayName("returns false when client is closed")
        void unhealthyWhenClosed() {
            PulsarClient client = mock(PulsarClient.class);
            when(client.isClosed()).thenReturn(true);

            PulsarConfig config = PulsarConfig.builder()
                    .client(client)
                    .topicPrefix("test-")
                    .build();
            PulsarBrokerConnector connector = new PulsarBrokerConnector(config);

            assertThat(connector.isHealthy()).isFalse();
        }
    }

    @Nested
    @DisplayName("close")
    class CloseTest {

        @Test
        @DisplayName("closes without error when no producers exist")
        void closesWithoutProducers() {
            PulsarClient client = mock(PulsarClient.class);
            PulsarConfig config = PulsarConfig.builder()
                    .client(client)
                    .topicPrefix("test-")
                    .build();
            PulsarBrokerConnector connector = new PulsarBrokerConnector(config);

            connector.close();

            assertThat(connector).isNotNull();
        }

        @Test
        @DisplayName("closes producers when they exist")
        void closesExistingProducers() throws PulsarClientException {
            PulsarClient client = mock(PulsarClient.class);
            ProducerBuilder<byte[]> producerBuilder = mock(ProducerBuilder.class);
            Producer<byte[]> producer = mock(Producer.class);
            TypedMessageBuilder<byte[]> messageBuilder = mock(TypedMessageBuilder.class);

            when(client.newProducer()).thenReturn(producerBuilder);
            when(producerBuilder.topic(anyString())).thenReturn(producerBuilder);
            when(producerBuilder.enableBatching(anyBoolean())).thenReturn(producerBuilder);
            when(producerBuilder.batchingMaxMessages(anyInt())).thenReturn(producerBuilder);
            when(producerBuilder.create()).thenReturn(producer);
            when(producer.newMessage()).thenReturn(messageBuilder);
            when(messageBuilder.key(anyString())).thenReturn(messageBuilder);
            when(messageBuilder.value(any(byte[].class))).thenReturn(messageBuilder);
            when(messageBuilder.property(anyString(), anyString())).thenReturn(messageBuilder);
            when(messageBuilder.send()).thenReturn(MessageId.earliest);

            PulsarConfig config = PulsarConfig.builder()
                    .client(client)
                    .topicPrefix("persistent://test/ns/outbox-")
                    .build();
            PulsarBrokerConnector connector = new PulsarBrokerConnector(config);

            connector.publish(createTestEvent());
            connector.close();

            verify(producer).close();
        }

        @Test
        @DisplayName("handles error when closing producer")
        void handlesCloseError() throws PulsarClientException {
            PulsarClient client = mock(PulsarClient.class);
            ProducerBuilder<byte[]> producerBuilder = mock(ProducerBuilder.class);
            Producer<byte[]> producer = mock(Producer.class);
            TypedMessageBuilder<byte[]> messageBuilder = mock(TypedMessageBuilder.class);

            when(client.newProducer()).thenReturn(producerBuilder);
            when(producerBuilder.topic(anyString())).thenReturn(producerBuilder);
            when(producerBuilder.enableBatching(anyBoolean())).thenReturn(producerBuilder);
            when(producerBuilder.batchingMaxMessages(anyInt())).thenReturn(producerBuilder);
            when(producerBuilder.create()).thenReturn(producer);
            when(producer.newMessage()).thenReturn(messageBuilder);
            when(messageBuilder.key(anyString())).thenReturn(messageBuilder);
            when(messageBuilder.value(any(byte[].class))).thenReturn(messageBuilder);
            when(messageBuilder.property(anyString(), anyString())).thenReturn(messageBuilder);
            when(messageBuilder.send()).thenReturn(MessageId.earliest);
            doThrow(new PulsarClientException("Close failed")).when(producer).close();

            PulsarConfig config = PulsarConfig.builder()
                    .client(client)
                    .topicPrefix("persistent://test/ns/outbox-")
                    .build();
            PulsarBrokerConnector connector = new PulsarBrokerConnector(config);

            connector.publish(createTestEvent());
            connector.close();

            verify(producer).close();
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
