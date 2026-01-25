package xyz.charks.outbox.micrometer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import xyz.charks.outbox.core.AggregateId;
import xyz.charks.outbox.core.EventType;
import xyz.charks.outbox.core.OutboxEvent;
import xyz.charks.outbox.core.OutboxEventId;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("OutboxMetrics")
class OutboxMetricsTest {

    private SimpleMeterRegistry registry;
    private OutboxMetrics metrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new OutboxMetrics(registry);
    }

    @Nested
    @DisplayName("constructor")
    class ConstructorTest {

        @Test
        @DisplayName("throws exception for null registry")
        void nullRegistry() {
            assertThatThrownBy(() -> new OutboxMetrics(null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("registry");
        }

        @Test
        @DisplayName("throws exception for null tags")
        void nullTags() {
            assertThatThrownBy(() -> new OutboxMetrics(registry, null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("tags");
        }

        @Test
        @DisplayName("registers all metrics")
        void registersAllMetrics() {
            assertThat(registry.find("outbox.events.published").counter()).isNotNull();
            assertThat(registry.find("outbox.events.failed").counter()).isNotNull();
            assertThat(registry.find("outbox.events.retried").counter()).isNotNull();
            assertThat(registry.find("outbox.events.exhausted").counter()).isNotNull();
            assertThat(registry.find("outbox.events.pending").gauge()).isNotNull();
            assertThat(registry.find("outbox.relay.batch.duration").timer()).isNotNull();
            assertThat(registry.find("outbox.relay.publish.duration").timer()).isNotNull();
            assertThat(registry.find("outbox.relay.fetch.duration").timer()).isNotNull();
            assertThat(registry.find("outbox.relay.batch.size").summary()).isNotNull();
        }

        @Test
        @DisplayName("applies custom tags")
        void appliesCustomTags() {
            SimpleMeterRegistry taggedRegistry = new SimpleMeterRegistry();
            Tags tags = Tags.of("app", "test-app");

            new OutboxMetrics(taggedRegistry, tags);

            Counter counter = taggedRegistry.find("outbox.events.published")
                    .tag("app", "test-app")
                    .counter();
            assertThat(counter).isNotNull();
        }
    }

    @Nested
    @DisplayName("onEventPublished")
    class OnEventPublishedTest {

        @Test
        @DisplayName("increments published counter")
        void incrementsCounter() {
            OutboxEvent event = createTestEvent();

            metrics.onEventPublished(event, Duration.ofMillis(50));
            metrics.onEventPublished(event, Duration.ofMillis(30));

            Counter counter = registry.find("outbox.events.published").counter();
            assertThat(counter.count()).isEqualTo(2.0);
        }

        @Test
        @DisplayName("records publish duration")
        void recordsDuration() {
            OutboxEvent event = createTestEvent();

            metrics.onEventPublished(event, Duration.ofMillis(100));

            Timer timer = registry.find("outbox.relay.publish.duration").timer();
            assertThat(timer.count()).isEqualTo(1);
            assertThat(timer.totalTime(java.util.concurrent.TimeUnit.MILLISECONDS)).isEqualTo(100.0);
        }
    }

    @Nested
    @DisplayName("onEventFailed")
    class OnEventFailedTest {

        @Test
        @DisplayName("increments failed counter")
        void incrementsCounter() {
            OutboxEvent event = createTestEvent();

            metrics.onEventFailed(event, new RuntimeException("Test error"));
            metrics.onEventFailed(event, new RuntimeException("Another error"));

            Counter counter = registry.find("outbox.events.failed").counter();
            assertThat(counter.count()).isEqualTo(2.0);
        }
    }

    @Nested
    @DisplayName("onEventRetryScheduled")
    class OnEventRetryScheduledTest {

        @Test
        @DisplayName("increments retried counter")
        void incrementsCounter() {
            OutboxEventId eventId = OutboxEventId.generate();

            metrics.onEventRetryScheduled(eventId, 1, Duration.ofSeconds(5));
            metrics.onEventRetryScheduled(eventId, 2, Duration.ofSeconds(10));

            Counter counter = registry.find("outbox.events.retried").counter();
            assertThat(counter.count()).isEqualTo(2.0);
        }
    }

    @Nested
    @DisplayName("onEventExhausted")
    class OnEventExhaustedTest {

        @Test
        @DisplayName("increments exhausted counter")
        void incrementsCounter() {
            OutboxEvent event = createTestEvent();

            metrics.onEventExhausted(event, 3, new RuntimeException("Final failure"));

            Counter counter = registry.find("outbox.events.exhausted").counter();
            assertThat(counter.count()).isEqualTo(1.0);
        }
    }

    @Nested
    @DisplayName("onBatchCompleted")
    class OnBatchCompletedTest {

        @Test
        @DisplayName("records batch duration")
        void recordsDuration() {
            metrics.onBatchCompleted(10, 8, 2, Duration.ofMillis(500));

            Timer timer = registry.find("outbox.relay.batch.duration").timer();
            assertThat(timer.count()).isEqualTo(1);
            assertThat(timer.totalTime(java.util.concurrent.TimeUnit.MILLISECONDS)).isEqualTo(500.0);
        }

        @Test
        @DisplayName("records batch size")
        void recordsBatchSize() {
            metrics.onBatchCompleted(10, 8, 2, Duration.ofMillis(500));
            metrics.onBatchCompleted(20, 18, 2, Duration.ofMillis(800));

            var summary = registry.find("outbox.relay.batch.size").summary();
            assertThat(summary.count()).isEqualTo(2);
            assertThat(summary.totalAmount()).isEqualTo(30.0);
        }
    }

    @Nested
    @DisplayName("onPollingCycleEnd")
    class OnPollingCycleEndTest {

        @Test
        @DisplayName("records fetch duration")
        void recordsFetchDuration() {
            metrics.onPollingCycleEnd(5, Duration.ofMillis(25));

            Timer timer = registry.find("outbox.relay.fetch.duration").timer();
            assertThat(timer.count()).isEqualTo(1);
            assertThat(timer.totalTime(java.util.concurrent.TimeUnit.MILLISECONDS)).isEqualTo(25.0);
        }
    }

    @Nested
    @DisplayName("updatePendingCount")
    class UpdatePendingCountTest {

        @Test
        @DisplayName("updates pending gauge")
        void updatesGauge() {
            metrics.updatePendingCount(42);

            Gauge gauge = registry.find("outbox.events.pending").gauge();
            assertThat(gauge.value()).isEqualTo(42.0);
        }

        @Test
        @DisplayName("allows multiple updates")
        void allowsMultipleUpdates() {
            metrics.updatePendingCount(10);
            metrics.updatePendingCount(25);
            metrics.updatePendingCount(5);

            Gauge gauge = registry.find("outbox.events.pending").gauge();
            assertThat(gauge.value()).isEqualTo(5.0);
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
