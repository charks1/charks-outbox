package xyz.charks.outbox.opentelemetry;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import xyz.charks.outbox.broker.BrokerConnector;
import xyz.charks.outbox.broker.PublishResult;
import xyz.charks.outbox.core.AggregateId;
import xyz.charks.outbox.core.EventType;
import xyz.charks.outbox.core.OutboxEvent;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

@DisplayName("OutboxTracingBrokerConnector")
class OutboxTracingBrokerConnectorTest {

    private InMemorySpanExporter spanExporter;
    private SdkTracerProvider tracerProvider;
    private BrokerConnector delegate;
    private OutboxTracing tracing;
    private OutboxTracingBrokerConnector connector;

    @BeforeEach
    void setUp() {
        spanExporter = InMemorySpanExporter.create();
        tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
                .build();
        OpenTelemetry openTelemetry = OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .build();

        delegate = mock(BrokerConnector.class);
        tracing = OutboxTracing.create(openTelemetry);
        connector = new OutboxTracingBrokerConnector(delegate, tracing, "kafka");
    }

    @AfterEach
    void tearDown() {
        spanExporter.reset();
        tracerProvider.close();
    }

    @Nested
    @DisplayName("constructor")
    class ConstructorTest {

        @Test
        @DisplayName("throws exception for null delegate")
        void nullDelegate() {
            assertThatThrownBy(() -> new OutboxTracingBrokerConnector(null, tracing, "kafka"))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("delegate");
        }

        @Test
        @DisplayName("throws exception for null tracing")
        void nullTracing() {
            assertThatThrownBy(() -> new OutboxTracingBrokerConnector(delegate, null, "kafka"))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("tracing");
        }

        @Test
        @DisplayName("throws exception for null brokerType")
        void nullBrokerType() {
            assertThatThrownBy(() -> new OutboxTracingBrokerConnector(delegate, tracing, null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("brokerType");
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
        @DisplayName("creates span and delegates publish")
        void createsSpanAndDelegates() {
            OutboxEvent event = createTestEvent();
            when(delegate.publish(event)).thenReturn(PublishResult.success(event.id()));

            PublishResult result = connector.publish(event);

            assertThat(result.success()).isTrue();
            verify(delegate).publish(event);

            List<SpanData> spans = spanExporter.getFinishedSpanItems();
            assertThat(spans).hasSize(1);

            SpanData spanData = spans.get(0);
            assertThat(spanData.getName()).isEqualTo("outbox.broker.publish");
            assertThat(spanData.getKind()).isEqualTo(SpanKind.CLIENT);
            assertThat(spanData.getStatus().getStatusCode()).isEqualTo(StatusCode.OK);
        }

        @Test
        @DisplayName("sets error status on failure result")
        void setsErrorStatusOnFailure() {
            OutboxEvent event = createTestEvent();
            when(delegate.publish(event)).thenReturn(PublishResult.failure(event.id(), "Connection failed"));

            PublishResult result = connector.publish(event);

            assertThat(result.success()).isFalse();

            List<SpanData> spans = spanExporter.getFinishedSpanItems();
            assertThat(spans).hasSize(1);
            SpanData spanData = spans.get(0);
            assertThat(spanData.getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
        }

        @Test
        @DisplayName("records exception on delegate throw")
        void recordsExceptionOnThrow() {
            OutboxEvent event = createTestEvent();
            RuntimeException exception = new RuntimeException("Broker unavailable");
            when(delegate.publish(event)).thenThrow(exception);

            assertThatThrownBy(() -> connector.publish(event))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessage("Broker unavailable");

            List<SpanData> spans = spanExporter.getFinishedSpanItems();
            assertThat(spans).hasSize(1);
            SpanData spanData = spans.get(0);
            assertThat(spanData.getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
            assertThat(spanData.getEvents()).hasSize(1);
            assertThat(spanData.getEvents().get(0).getName()).isEqualTo("exception");
        }
    }

    @Nested
    @DisplayName("isHealthy")
    class IsHealthyTest {

        @Test
        @DisplayName("delegates to underlying connector")
        void delegatesHealthCheck() {
            when(delegate.isHealthy()).thenReturn(true);

            assertThat(connector.isHealthy()).isTrue();
            verify(delegate).isHealthy();
        }

        @Test
        @DisplayName("returns false when delegate is unhealthy")
        void returnsFalseWhenUnhealthy() {
            when(delegate.isHealthy()).thenReturn(false);

            assertThat(connector.isHealthy()).isFalse();
        }
    }

    @Nested
    @DisplayName("close")
    class CloseTest {

        @Test
        @DisplayName("delegates close to underlying connector")
        void delegatesClose() {
            connector.close();

            verify(delegate).close();
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
