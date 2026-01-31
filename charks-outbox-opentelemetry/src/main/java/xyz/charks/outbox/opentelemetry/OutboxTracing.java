package xyz.charks.outbox.opentelemetry;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.charks.outbox.core.OutboxEvent;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * OpenTelemetry tracing integration for the outbox relay.
 *
 * <p>This class provides distributed tracing capabilities for outbox event
 * processing, allowing visibility into event publication flows across services.
 *
 * <p>Spans are created for:
 * <ul>
 *   <li>Poll cycles - parent span for each relay poll iteration</li>
 *   <li>Event processing - individual span per event within a poll cycle</li>
 *   <li>Broker publish - child span for the actual broker communication</li>
 * </ul>
 *
 * @see OutboxTracingBrokerConnector
 */
public record OutboxTracing(Tracer tracer) {

    private static final Logger LOG = LoggerFactory.getLogger(OutboxTracing.class);

    private static final String INSTRUMENTATION_NAME = "xyz.charks.outbox";
    private static final String INSTRUMENTATION_VERSION = "1.0.0";

    private static final AttributeKey<String> EVENT_ID = AttributeKey.stringKey("outbox.event.id");
    private static final AttributeKey<String> AGGREGATE_TYPE = AttributeKey.stringKey("outbox.aggregate.type");
    private static final AttributeKey<String> AGGREGATE_ID = AttributeKey.stringKey("outbox.aggregate.id");
    private static final AttributeKey<String> EVENT_TYPE = AttributeKey.stringKey("outbox.event.type");
    private static final AttributeKey<Long> BATCH_SIZE = AttributeKey.longKey("outbox.batch.size");

    /**
     * Creates a new OutboxTracing instance from an OpenTelemetry instance.
     *
     * @param openTelemetry the OpenTelemetry instance
     * @return a new OutboxTracing instance
     */
    public static OutboxTracing create(OpenTelemetry openTelemetry) {
        Objects.requireNonNull(openTelemetry, "openTelemetry");
        Tracer tracer = openTelemetry.getTracer(INSTRUMENTATION_NAME, INSTRUMENTATION_VERSION);
        LOG.debug("Initialized OpenTelemetry outbox tracing");
        return new OutboxTracing(tracer);
    }

    /**
     * Creates a span for a poll cycle.
     *
     * @param batchSize the number of events in the batch
     * @return the created span
     */
    public Span startPollSpan(int batchSize) {
        return tracer.spanBuilder("outbox.relay.poll")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute(BATCH_SIZE, (long) batchSize)
                .startSpan();
    }

    /**
     * Creates a span for processing an individual event.
     *
     * @param event         the outbox event
     * @param parentContext the parent context
     * @return the created span
     */
    public Span startEventSpan(OutboxEvent event, Context parentContext) {
        Objects.requireNonNull(event, "event");

        return tracer.spanBuilder("outbox.event.process")
                .setParent(parentContext)
                .setSpanKind(SpanKind.PRODUCER)
                .setAttribute(EVENT_ID, event.id().value().toString())
                .setAttribute(AGGREGATE_TYPE, event.aggregateType())
                .setAttribute(AGGREGATE_ID, event.aggregateId().value())
                .setAttribute(EVENT_TYPE, event.eventType().value())
                .startSpan();
    }

    /**
     * Creates a span for the broker publish operation.
     *
     * @param event         the outbox event
     * @param brokerType    the type of broker (e.g., "kafka", "rabbitmq")
     * @param parentContext the parent context
     * @return the created span
     */
    public Span startPublishSpan(OutboxEvent event, String brokerType, Context parentContext) {
        Objects.requireNonNull(event, "event");
        Objects.requireNonNull(brokerType, "brokerType");

        return tracer.spanBuilder("outbox.broker.publish")
                .setParent(parentContext)
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute(EVENT_ID, event.id().value().toString())
                .setAttribute(AttributeKey.stringKey("messaging.system"), brokerType)
                .startSpan();
    }

    /**
     * Executes a supplier within a span scope.
     *
     * @param span     the span to make current
     * @param supplier the supplier to execute
     * @param <T>      the return type
     * @return the supplier result
     */
    public <T> T executeInSpan(Span span, Supplier<T> supplier) {
        try (var _ = span.makeCurrent()) {
            return supplier.get();
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
            throw e;
        } finally {
            span.end();
        }
    }

}
