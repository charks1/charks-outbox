package xyz.charks.outbox.kafka;

import xyz.charks.outbox.broker.PublishResult;
import xyz.charks.outbox.core.AggregateId;
import xyz.charks.outbox.core.EventType;
import xyz.charks.outbox.core.OutboxEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("KafkaBrokerConnector")
class KafkaBrokerConnectorTest {

    @Mock
    private KafkaProducer<String, byte[]> producer;

    private KafkaBrokerConnector connector;

    @BeforeEach
    void setup() {
        connector = new KafkaBrokerConnector(producer, Duration.ofSeconds(10));
    }

    @Nested
    @DisplayName("publish")
    class PublishTest {

        @Test
        @DisplayName("publishes event successfully")
        void publishesSuccessfully() {
            OutboxEvent event = createTestEvent();
            RecordMetadata metadata = new RecordMetadata(
                    new TopicPartition("orders", 2), 0, 42, 0, 0, 0
            );
            when(producer.send(any())).thenReturn(CompletableFuture.completedFuture(metadata));

            PublishResult result = connector.publish(event);

            assertThat(result.success()).isTrue();
            assertThat(result.eventId()).isEqualTo(event.id());
            assertThat(result.partition()).isEqualTo(2);
            assertThat(result.offset()).isEqualTo(42);
        }

        @Test
        @DisplayName("includes headers in producer record")
        void includesHeaders() {
            OutboxEvent event = OutboxEvent.builder()
                    .aggregateType("Order")
                    .aggregateId(AggregateId.of("order-123"))
                    .eventType(EventType.of("OrderCreated"))
                    .topic("orders")
                    .payload("{}".getBytes(StandardCharsets.UTF_8))
                    .headers(Map.of("correlation-id", "abc-123"))
                    .build();

            RecordMetadata metadata = new RecordMetadata(
                    new TopicPartition("orders", 0), 0, 0, 0, 0, 0
            );
            when(producer.send(any())).thenReturn(CompletableFuture.completedFuture(metadata));

            connector.publish(event);

            ArgumentCaptor<ProducerRecord<String, byte[]>> captor =
                    ArgumentCaptor.forClass(ProducerRecord.class);
            verify(producer).send(captor.capture());

            ProducerRecord<String, byte[]> producerRecord = captor.getValue();
            assertThat(producerRecord.topic()).isEqualTo("orders");
            assertThat(producerRecord.key()).isNull();
            assertThat(producerRecord.value()).isEqualTo("{}".getBytes(StandardCharsets.UTF_8));
            assertThat(producerRecord.headers().lastHeader("correlation-id")).isNotNull();
            assertThat(producerRecord.headers().lastHeader("outbox-event-id")).isNotNull();
            assertThat(producerRecord.headers().lastHeader("outbox-event-type")).isNotNull();
            assertThat(producerRecord.headers().lastHeader("outbox-aggregate-type")).isNotNull();
            assertThat(producerRecord.headers().lastHeader("outbox-aggregate-id")).isNotNull();
        }

        @Test
        @DisplayName("uses partition key when provided")
        void usesPartitionKey() {
            OutboxEvent event = OutboxEvent.builder()
                    .aggregateType("Order")
                    .aggregateId(AggregateId.of("order-123"))
                    .eventType(EventType.of("OrderCreated"))
                    .topic("orders")
                    .partitionKey("order-123")
                    .payload("{}".getBytes(StandardCharsets.UTF_8))
                    .build();

            RecordMetadata metadata = new RecordMetadata(
                    new TopicPartition("orders", 0), 0, 0, 0, 0, 0
            );
            when(producer.send(any())).thenReturn(CompletableFuture.completedFuture(metadata));

            connector.publish(event);

            ArgumentCaptor<ProducerRecord<String, byte[]>> captor =
                    ArgumentCaptor.forClass(ProducerRecord.class);
            verify(producer).send(captor.capture());

            assertThat(captor.getValue().key()).isEqualTo("order-123");
        }

        @Test
        @DisplayName("returns failure on send exception")
        void returnsFailureOnException() {
            OutboxEvent event = createTestEvent();
            CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
            future.completeExceptionally(new TimeoutException("Broker not available"));
            when(producer.send(any())).thenReturn(future);

            PublishResult result = connector.publish(event);

            assertThat(result.success()).isFalse();
            assertThat(result.error()).contains("Broker not available");
        }

        @Test
        @DisplayName("rejects null event")
        void rejectsNullEvent() {
            assertThatThrownBy(() -> connector.publish(null))
                    .isInstanceOf(NullPointerException.class);
        }
    }

    @Nested
    @DisplayName("publishAll")
    class PublishAllTest {

        @Test
        @DisplayName("publishes multiple events")
        void publishesMultiple() {
            OutboxEvent event1 = createTestEvent();
            OutboxEvent event2 = createTestEvent();

            RecordMetadata metadata1 = new RecordMetadata(
                    new TopicPartition("orders", 0), 0, 10, 0, 0, 0
            );
            RecordMetadata metadata2 = new RecordMetadata(
                    new TopicPartition("orders", 1), 0, 20, 0, 0, 0
            );

            when(producer.send(any()))
                    .thenReturn(CompletableFuture.completedFuture(metadata1))
                    .thenReturn(CompletableFuture.completedFuture(metadata2));

            List<PublishResult> results = connector.publishAll(List.of(event1, event2));

            assertThat(results).hasSize(2);
            assertThat(results.get(0).success()).isTrue();
            assertThat(results.get(0).offset()).isEqualTo(10);
            assertThat(results.get(1).success()).isTrue();
            assertThat(results.get(1).offset()).isEqualTo(20);

            verify(producer).flush();
        }

        @Test
        @DisplayName("returns empty list for empty input")
        void emptyInput() {
            List<PublishResult> results = connector.publishAll(List.of());
            assertThat(results).isEmpty();
        }

        @Test
        @DisplayName("continues on partial failure")
        void continuesOnPartialFailure() {
            OutboxEvent event1 = createTestEvent();
            OutboxEvent event2 = createTestEvent();

            RecordMetadata metadata = new RecordMetadata(
                    new TopicPartition("orders", 0), 0, 10, 0, 0, 0
            );

            CompletableFuture<RecordMetadata> failedFuture = new CompletableFuture<>();
            failedFuture.completeExceptionally(new RuntimeException("Failed"));

            when(producer.send(any()))
                    .thenReturn(failedFuture)
                    .thenReturn(CompletableFuture.completedFuture(metadata));

            List<PublishResult> results = connector.publishAll(List.of(event1, event2));

            assertThat(results).hasSize(2);
            assertThat(results.get(0).success()).isFalse();
            assertThat(results.get(1).success()).isTrue();
        }
    }

    @Nested
    @DisplayName("isHealthy")
    class IsHealthyTest {

        @Test
        @DisplayName("returns true when broker is reachable")
        void healthyWhenReachable() {
            when(producer.partitionsFor(any())).thenReturn(List.of());

            assertThat(connector.isHealthy()).isTrue();
        }

        @Test
        @DisplayName("returns false when broker is unreachable")
        void unhealthyWhenUnreachable() {
            when(producer.partitionsFor(any())).thenThrow(new TimeoutException("Connection failed"));

            assertThat(connector.isHealthy()).isFalse();
        }
    }

    @Nested
    @DisplayName("close")
    class CloseTest {

        @Test
        @DisplayName("flushes and closes producer")
        void flushesAndCloses() {
            connector.close();

            verify(producer).flush();
            verify(producer).close(Duration.ofSeconds(30));
        }
    }

    @Nested
    @DisplayName("constructor")
    class ConstructorTest {

        @Test
        @DisplayName("rejects null producer")
        void rejectsNullProducer() {
            assertThatThrownBy(() -> new KafkaBrokerConnector(null, Duration.ofSeconds(10)))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        @DisplayName("rejects null timeout")
        void rejectsNullTimeout() {
            assertThatThrownBy(() -> new KafkaBrokerConnector(producer, null))
                    .isInstanceOf(NullPointerException.class);
        }
    }

    private OutboxEvent createTestEvent() {
        return OutboxEvent.builder()
                .aggregateType("Order")
                .aggregateId(AggregateId.of("order-" + System.nanoTime()))
                .eventType(EventType.of("OrderCreated"))
                .topic("orders")
                .payload("{\"test\":true}".getBytes(StandardCharsets.UTF_8))
                .build();
    }
}
