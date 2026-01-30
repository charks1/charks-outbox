package xyz.charks.outbox.sqs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;
import xyz.charks.outbox.broker.BrokerConnector;
import xyz.charks.outbox.broker.PublishResult;
import xyz.charks.outbox.core.OutboxEvent;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * AWS SQS implementation of {@link BrokerConnector}.
 *
 * <p>This connector publishes outbox events to an SQS queue with
 * event metadata as message attributes.
 *
 * @see SqsConfig
 */
public class SqsBrokerConnector implements BrokerConnector, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(SqsBrokerConnector.class);
    private static final String DATA_TYPE_STRING = "String";

    private final SqsConfig config;

    /**
     * Creates a new SQS broker connector.
     *
     * @param config the connector configuration
     */
    public SqsBrokerConnector(SqsConfig config) {
        this.config = Objects.requireNonNull(config, "config");
        LOG.info("Initialized SQS broker connector with queue: {}", config.queueUrl());
    }

    @Override
    public PublishResult publish(OutboxEvent event) {
        Objects.requireNonNull(event, "event");

        try {
            Map<String, MessageAttributeValue> attributes = buildAttributes(event);

            String body = Base64.getEncoder().encodeToString(event.payload());

            SendMessageRequest.Builder requestBuilder = SendMessageRequest.builder()
                .queueUrl(config.queueUrl())
                .messageBody(body)
                .messageAttributes(attributes)
                .delaySeconds(config.delaySeconds());

            // Only set FIFO attributes if the queue is FIFO
            if (config.queueUrl().endsWith(".fifo")) {
                requestBuilder
                    .messageGroupId(event.aggregateId().value())
                    .messageDeduplicationId(event.id().value().toString());
            }

            config.client().sendMessage(requestBuilder.build());

            LOG.debug("Published event {} to SQS queue '{}'", event.id(), config.queueUrl());
            return PublishResult.success(event.id());
        } catch (SqsException e) {
            LOG.error("Failed to publish event {} to SQS", event.id(), e);
            return PublishResult.failure(event.id(), e);
        }
    }

    @Override
    public boolean isHealthy() {
        try {
            config.client().getQueueAttributes(
                GetQueueAttributesRequest.builder()
                    .queueUrl(config.queueUrl())
                    .build()
            );
            return true;
        } catch (SqsException | IllegalStateException e) {
            LOG.debug("SQS health check failed", e);
            return false;
        }
    }

    private Map<String, MessageAttributeValue> buildAttributes(OutboxEvent event) {
        Map<String, MessageAttributeValue> attributes = new HashMap<>();

        attributes.put("eventId", MessageAttributeValue.builder()
            .dataType(DATA_TYPE_STRING)
            .stringValue(event.id().value().toString())
            .build());

        attributes.put("aggregateType", MessageAttributeValue.builder()
            .dataType(DATA_TYPE_STRING)
            .stringValue(event.aggregateType())
            .build());

        attributes.put("aggregateId", MessageAttributeValue.builder()
            .dataType(DATA_TYPE_STRING)
            .stringValue(event.aggregateId().value())
            .build());

        attributes.put("eventType", MessageAttributeValue.builder()
            .dataType(DATA_TYPE_STRING)
            .stringValue(event.eventType().value())
            .build());

        attributes.put("createdAt", MessageAttributeValue.builder()
            .dataType(DATA_TYPE_STRING)
            .stringValue(event.createdAt().toString())
            .build());

        event.headers().forEach((key, value) ->
            attributes.put(key, MessageAttributeValue.builder()
                .dataType(DATA_TYPE_STRING)
                .stringValue(value)
                .build()));

        return attributes;
    }

    @Override
    public void close() {
        try {
            config.client().close();
            LOG.info("Closed SQS broker connector");
        } catch (Exception e) {
            LOG.warn("Error closing SQS client", e);
        }
    }
}
