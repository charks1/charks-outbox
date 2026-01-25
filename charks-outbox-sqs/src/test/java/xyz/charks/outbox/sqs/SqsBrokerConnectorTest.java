package xyz.charks.outbox.sqs;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;
import software.amazon.awssdk.services.sqs.model.SqsException;
import xyz.charks.outbox.broker.PublishResult;
import xyz.charks.outbox.core.AggregateId;
import xyz.charks.outbox.core.EventType;
import xyz.charks.outbox.core.OutboxEvent;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@DisplayName("SqsBrokerConnector")
class SqsBrokerConnectorTest {

    private SqsClient sqsClient;
    private SqsBrokerConnector connector;

    @BeforeEach
    void setUp() {
        sqsClient = mock(SqsClient.class);

        SqsConfig config = SqsConfig.builder()
                .client(sqsClient)
                .queueUrl("https://sqs.us-east-1.amazonaws.com/123456789/test-queue")
                .build();

        connector = new SqsBrokerConnector(config);
    }

    @Nested
    @DisplayName("constructor")
    class ConstructorTest {

        @Test
        @DisplayName("throws exception for null config")
        void nullConfig() {
            assertThatThrownBy(() -> new SqsBrokerConnector(null))
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
        @DisplayName("publishes event to SQS queue")
        void publishesEvent() {
            OutboxEvent event = createTestEvent();
            when(sqsClient.sendMessage(any(SendMessageRequest.class)))
                    .thenReturn(SendMessageResponse.builder().messageId("msg-123").build());

            PublishResult result = connector.publish(event);

            assertThat(result.success()).isTrue();
            assertThat(result.eventId()).isEqualTo(event.id());
            verify(sqsClient).sendMessage(any(SendMessageRequest.class));
        }

        @Test
        @DisplayName("returns failure when SQS throws exception")
        void publishFailure() {
            OutboxEvent event = createTestEvent();
            when(sqsClient.sendMessage(any(SendMessageRequest.class)))
                    .thenThrow(SqsException.builder().message("Queue not found").build());

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
        @DisplayName("returns true when queue is accessible")
        void healthyWhenAccessible() {
            when(sqsClient.getQueueAttributes(any(GetQueueAttributesRequest.class)))
                    .thenReturn(GetQueueAttributesResponse.builder().build());

            assertThat(connector.isHealthy()).isTrue();
        }

        @Test
        @DisplayName("returns false when queue is not accessible")
        void unhealthyWhenNotAccessible() {
            when(sqsClient.getQueueAttributes(any(GetQueueAttributesRequest.class)))
                    .thenThrow(SqsException.builder().message("Access denied").build());

            assertThat(connector.isHealthy()).isFalse();
        }
    }

    @Nested
    @DisplayName("close")
    class CloseTest {

        @Test
        @DisplayName("closes SQS client")
        void closesClient() {
            connector.close();

            verify(sqsClient).close();
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
