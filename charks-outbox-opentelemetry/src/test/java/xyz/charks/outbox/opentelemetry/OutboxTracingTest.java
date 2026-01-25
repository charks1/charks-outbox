package xyz.charks.outbox.opentelemetry;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
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
import xyz.charks.outbox.core.AggregateId;
import xyz.charks.outbox.core.EventType;
import xyz.charks.outbox.core.OutboxEvent;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("OutboxTracing")
class OutboxTracingTest {

    private InMemorySpanExporter spanExporter;
    private SdkTracerProvider tracerProvider;
    private OpenTelemetry openTelemetry;
    private OutboxTracing tracing;

    @BeforeEach
    void setUp() {
        spanExporter = InMemorySpanExporter.create();
        tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
                .build();
        openTelemetry = OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .build();
        tracing = new OutboxTracing(openTelemetry);
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
        @DisplayName("throws exception for null openTelemetry")
        void nullOpenTelemetry() {
            assertThatThrownBy(() -> new OutboxTracing(null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("openTelemetry");
        }
    }

    @Nested
    @DisplayName("startPollSpan")
    class StartPollSpanTest {

        @Test
        @DisplayName("creates span with batch size attribute")
        void createsSpan() {
            Span span = tracing.startPollSpan(10);
            span.end();

            List<SpanData> spans = spanExporter.getFinishedSpanItems();
            assertThat(spans).hasSize(1);

            SpanData spanData = spans.get(0);
            assertThat(spanData.getName()).isEqualTo("outbox.relay.poll");
            assertThat(spanData.getKind()).isEqualTo(SpanKind.INTERNAL);
            assertThat(spanData.getAttributes().get(io.opentelemetry.api.common.AttributeKey.longKey("outbox.batch.size")))
                    .isEqualTo(10L);
        }
    }

    @Nested
    @DisplayName("startEventSpan")
    class StartEventSpanTest {

        @Test
        @DisplayName("throws exception for null event")
        void nullEvent() {
            assertThatThrownBy(() -> tracing.startEventSpan(null, Context.current()))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("event");
        }

        @Test
        @DisplayName("creates span with event attributes")
        void createsSpanWithAttributes() {
            OutboxEvent event = createTestEvent();

            Span span = tracing.startEventSpan(event, Context.current());
            span.end();

            List<SpanData> spans = spanExporter.getFinishedSpanItems();
            assertThat(spans).hasSize(1);

            SpanData spanData = spans.get(0);
            assertThat(spanData.getName()).isEqualTo("outbox.event.process");
            assertThat(spanData.getKind()).isEqualTo(SpanKind.PRODUCER);
            assertThat(spanData.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("outbox.aggregate.type")))
                    .isEqualTo("Order");
            assertThat(spanData.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("outbox.aggregate.id")))
                    .isEqualTo("order-123");
            assertThat(spanData.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("outbox.event.type")))
                    .isEqualTo("OrderCreated");
        }
    }

    @Nested
    @DisplayName("startPublishSpan")
    class StartPublishSpanTest {

        @Test
        @DisplayName("throws exception for null event")
        void nullEvent() {
            assertThatThrownBy(() -> tracing.startPublishSpan(null, "kafka", Context.current()))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("event");
        }

        @Test
        @DisplayName("throws exception for null brokerType")
        void nullBrokerType() {
            OutboxEvent event = createTestEvent();
            assertThatThrownBy(() -> tracing.startPublishSpan(event, null, Context.current()))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("brokerType");
        }

        @Test
        @DisplayName("creates span with broker type attribute")
        void createsSpanWithBrokerType() {
            OutboxEvent event = createTestEvent();

            Span span = tracing.startPublishSpan(event, "kafka", Context.current());
            span.end();

            List<SpanData> spans = spanExporter.getFinishedSpanItems();
            assertThat(spans).hasSize(1);

            SpanData spanData = spans.get(0);
            assertThat(spanData.getName()).isEqualTo("outbox.broker.publish");
            assertThat(spanData.getKind()).isEqualTo(SpanKind.CLIENT);
            assertThat(spanData.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("messaging.system")))
                    .isEqualTo("kafka");
        }
    }

    @Nested
    @DisplayName("executeInSpan")
    class ExecuteInSpanTest {

        @Test
        @DisplayName("executes supplier and returns result")
        void executesSupplier() {
            Span span = tracing.startPollSpan(5);

            String result = tracing.executeInSpan(span, () -> "test-result");

            assertThat(result).isEqualTo("test-result");
        }

        @Test
        @DisplayName("executes runnable successfully")
        void executesRunnable() {
            Span span = tracing.startPollSpan(5);
            boolean[] executed = {false};

            tracing.executeInSpan(span, () -> executed[0] = true);

            assertThat(executed[0]).isTrue();
        }

        @Test
        @DisplayName("records exception on failure")
        void recordsException() {
            Span span = tracing.startPollSpan(5);

            assertThatThrownBy(() -> tracing.executeInSpan(span, () -> {
                throw new RuntimeException("Test error");
            })).isInstanceOf(RuntimeException.class);

            List<SpanData> spans = spanExporter.getFinishedSpanItems();
            assertThat(spans).hasSize(1);
            SpanData spanData = spans.get(0);
            assertThat(spanData.getEvents()).hasSize(1);
            assertThat(spanData.getEvents().get(0).getName()).isEqualTo("exception");
        }
    }

    @Nested
    @DisplayName("tracer")
    class TracerTest {

        @Test
        @DisplayName("returns non-null tracer")
        void returnsTracer() {
            assertThat(tracing.tracer()).isNotNull();
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
