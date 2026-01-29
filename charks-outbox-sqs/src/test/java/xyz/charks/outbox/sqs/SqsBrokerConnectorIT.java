package xyz.charks.outbox.sqs;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import xyz.charks.outbox.broker.PublishResult;
import xyz.charks.outbox.core.AggregateId;
import xyz.charks.outbox.core.EventType;
import xyz.charks.outbox.core.OutboxEvent;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.SQS;

/**
 * Integration tests for SqsBrokerConnector using Testcontainers with LocalStack.
 */
@Testcontainers
@DisplayName("SqsBrokerConnector Integration Tests")
class SqsBrokerConnectorIT {

    @Container
    static LocalStackContainer localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.0"))
            .withServices(SQS);

    private SqsClient sqsClient;
    private SqsBrokerConnector connector;
    private String queueUrl;

    @BeforeEach
    void setUp() {
        sqsClient = SqsClient.builder()
                .endpointOverride(localstack.getEndpointOverride(SQS))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())))
                .region(Region.of(localstack.getRegion()))
                .build();

        String queueName = "test-queue-" + UUID.randomUUID();
        queueUrl = sqsClient.createQueue(CreateQueueRequest.builder()
                .queueName(queueName)
                .build()).queueUrl();

        SqsConfig config = SqsConfig.builder()
                .client(sqsClient)
                .queueUrl(queueUrl)
                .delaySeconds(0)
                .build();
        connector = new SqsBrokerConnector(config);
    }

    @AfterEach
    void tearDown() {
        if (connector != null) {
            connector.close();
        }
        if (sqsClient != null && queueUrl != null) {
            try {
                sqsClient.deleteQueue(DeleteQueueRequest.builder().queueUrl(queueUrl).build());
            } catch (Exception ignored) {
            }
        }
    }

    @Nested
    @DisplayName("publish")
    class PublishIT {

        @Test
        @DisplayName("publishes event to SQS queue and receives it")
        void publishesEventToSqs() {
            OutboxEvent event = createTestEvent();

            PublishResult result = connector.publish(event);

            assertThat(result.success()).isTrue();
            assertThat(result.eventId()).isEqualTo(event.id());

            AtomicReference<Message> receivedMessage = new AtomicReference<>();
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                List<Message> messages = sqsClient.receiveMessage(ReceiveMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .maxNumberOfMessages(1)
                        .messageAttributeNames("All")
                        .waitTimeSeconds(5)
                        .build()).messages();

                if (!messages.isEmpty()) {
                    receivedMessage.set(messages.getFirst());
                }
                assertThat(receivedMessage.get()).isNotNull();
            });

            Message message = receivedMessage.get();
            byte[] payload = Base64.getDecoder().decode(message.body());
            assertThat(payload).isEqualTo(event.payload());
        }

        @Test
        @DisplayName("includes event metadata as message attributes")
        void includesEventMetadataAsAttributes() {
            OutboxEvent event = OutboxEvent.builder()
                    .aggregateType("Order")
                    .aggregateId(AggregateId.of("order-123"))
                    .eventType(EventType.of("OrderCreated"))
                    .topic("orders")
                    .payload("{\"amount\":100}".getBytes(StandardCharsets.UTF_8))
                    .headers(Map.of("traceId", "trace-abc"))
                    .build();

            connector.publish(event);

            AtomicReference<Message> receivedMessage = new AtomicReference<>();
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                List<Message> messages = sqsClient.receiveMessage(ReceiveMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .maxNumberOfMessages(1)
                        .messageAttributeNames("All")
                        .waitTimeSeconds(5)
                        .build()).messages();

                if (!messages.isEmpty()) {
                    receivedMessage.set(messages.getFirst());
                }
                assertThat(receivedMessage.get()).isNotNull();
            });

            Message message = receivedMessage.get();
            var attrs = message.messageAttributes();

            assertThat(attrs.get("eventId").stringValue()).isEqualTo(event.id().value().toString());
            assertThat(attrs.get("aggregateType").stringValue()).isEqualTo("Order");
            assertThat(attrs.get("aggregateId").stringValue()).isEqualTo("order-123");
            assertThat(attrs.get("eventType").stringValue()).isEqualTo("OrderCreated");
            assertThat(attrs.get("createdAt").stringValue()).isEqualTo(event.createdAt().toString());
            assertThat(attrs.get("traceId").stringValue()).isEqualTo("trace-abc");
        }
    }

    @Nested
    @DisplayName("publish with FIFO queue")
    class PublishFifoIT {

        @Test
        @DisplayName("publishes event to FIFO queue with deduplication")
        void publishesToFifoQueue() {
            String fifoQueueName = "test-queue-" + UUID.randomUUID() + ".fifo";
            String fifoQueueUrl = sqsClient.createQueue(CreateQueueRequest.builder()
                    .queueName(fifoQueueName)
                    .attributes(Map.of(
                            QueueAttributeName.FIFO_QUEUE, "true",
                            QueueAttributeName.CONTENT_BASED_DEDUPLICATION, "false"
                    ))
                    .build()).queueUrl();

            SqsConfig fifoConfig = SqsConfig.builder()
                    .client(sqsClient)
                    .queueUrl(fifoQueueUrl)
                    .build();

            try (SqsBrokerConnector fifoConnector = new SqsBrokerConnector(fifoConfig)) {
                OutboxEvent event = createTestEvent();

                PublishResult result = fifoConnector.publish(event);

                assertThat(result.success()).isTrue();

                AtomicReference<Message> receivedMessage = new AtomicReference<>();
                await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                    List<Message> messages = sqsClient.receiveMessage(ReceiveMessageRequest.builder()
                            .queueUrl(fifoQueueUrl)
                            .maxNumberOfMessages(1)
                            .messageAttributeNames("All")
                            .waitTimeSeconds(5)
                            .build()).messages();

                    if (!messages.isEmpty()) {
                        receivedMessage.set(messages.getFirst());
                    }
                    assertThat(receivedMessage.get()).isNotNull();
                });

                assertThat(receivedMessage.get()).isNotNull();
            } finally {
                sqsClient.deleteQueue(DeleteQueueRequest.builder().queueUrl(fifoQueueUrl).build());
            }
        }
    }

    @Nested
    @DisplayName("isHealthy")
    class IsHealthyIT {

        @Test
        @DisplayName("returns true when SQS is accessible")
        void returnsTrueWhenAccessible() {
            boolean healthy = connector.isHealthy();

            assertThat(healthy).isTrue();
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
