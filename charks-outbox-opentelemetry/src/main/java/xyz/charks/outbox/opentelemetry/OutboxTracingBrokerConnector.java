package xyz.charks.outbox.opentelemetry;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import xyz.charks.outbox.broker.BrokerConnector;
import xyz.charks.outbox.broker.PublishResult;
import xyz.charks.outbox.core.OutboxEvent;

import java.util.Objects;

/**
 * A {@link BrokerConnector} decorator that adds OpenTelemetry tracing.
 *
 * <p>This wrapper adds distributed tracing to any broker connector,
 * creating spans for each publish operation.
 *
 * @see OutboxTracing
 * @see BrokerConnector
 */
public class OutboxTracingBrokerConnector implements BrokerConnector, AutoCloseable {

    private final BrokerConnector delegate;
    private final OutboxTracing tracing;
    private final String brokerType;

    /**
     * Creates a new tracing broker connector.
     *
     * @param delegate the underlying broker connector
     * @param tracing the outbox tracing instance
     * @param brokerType the broker type identifier (e.g., "kafka", "rabbitmq")
     */
    public OutboxTracingBrokerConnector(BrokerConnector delegate, OutboxTracing tracing, String brokerType) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.tracing = Objects.requireNonNull(tracing, "tracing");
        this.brokerType = Objects.requireNonNull(brokerType, "brokerType");
    }

    @Override
    public PublishResult publish(OutboxEvent event) {
        Objects.requireNonNull(event, "event");

        Span span = tracing.startPublishSpan(event, brokerType, Context.current());

        try (Scope ignored = span.makeCurrent()) {
            PublishResult result = delegate.publish(event);

            if (result.success()) {
                span.setStatus(StatusCode.OK);
            } else {
                span.setStatus(StatusCode.ERROR, result.getError().orElse("Unknown error"));
            }

            return result;
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
            throw e;
        } finally {
            span.end();
        }
    }

    @Override
    public boolean isHealthy() {
        return delegate.isHealthy();
    }

    @Override
    public void close() {
        delegate.close();
    }
}
