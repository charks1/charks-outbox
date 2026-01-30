package xyz.charks.outbox.core;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OutboxEventTest {

    @Test
    void shouldBuildMinimalEvent() {
        OutboxEvent event = OutboxEvent.builder()
                .aggregateType("Order")
                .aggregateId("order-123")
                .eventType("OrderCreated")
                .topic("orders")
                .payload(new byte[]{1, 2, 3})
                .build();

        assertThat(event.id()).isNotNull();
        assertThat(event.aggregateType()).isEqualTo("Order");
        assertThat(event.aggregateId().value()).isEqualTo("order-123");
        assertThat(event.eventType().value()).isEqualTo("OrderCreated");
        assertThat(event.topic()).isEqualTo("orders");
        assertThat(event.payload()).containsExactly(1, 2, 3);
        assertThat(event.status()).isInstanceOf(Pending.class);
        assertThat(event.retryCount()).isZero();
        assertThat(event.headers()).isEmpty();
    }

    @Test
    void shouldBuildFullyConfiguredEvent() {
        OutboxEventId id = OutboxEventId.generate();
        Instant createdAt = Instant.parse("2024-01-15T10:30:00Z");
        Map<String, String> headers = Map.of("correlation-id", "abc-123");

        OutboxEvent event = OutboxEvent.builder()
                .id(id)
                .aggregateType("Order")
                .aggregateId(AggregateId.of("order-456"))
                .eventType(EventType.of("OrderShipped"))
                .topic("orders")
                .partitionKey("customer-789")
                .payload(new byte[]{4, 5, 6})
                .headers(headers)
                .createdAt(createdAt)
                .build();

        assertThat(event.id()).isEqualTo(id);
        assertThat(event.partitionKey()).isEqualTo("customer-789");
        assertThat(event.headers()).containsEntry("correlation-id", "abc-123");
        assertThat(event.createdAt()).isEqualTo(createdAt);
    }

    @Test
    void shouldAddHeadersIndividually() {
        OutboxEvent event = OutboxEvent.builder()
                .aggregateType("Order")
                .aggregateId("123")
                .eventType("OrderCreated")
                .topic("orders")
                .payload(new byte[0])
                .header("key1", "value1")
                .header("key2", "value2")
                .build();

        assertThat(event.headers())
                .containsEntry("key1", "value1")
                .containsEntry("key2", "value2");
    }

    @Test
    void shouldRejectMissingAggregateType() {
        assertThatThrownBy(() -> OutboxEvent.builder()
                .aggregateId("123")
                .eventType("Event")
                .topic("topic")
                .payload(new byte[0])
                .build())
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Aggregate type");
    }

    @Test
    void shouldRejectBlankAggregateType() {
        assertThatThrownBy(() -> OutboxEvent.builder()
                .aggregateType("  ")
                .aggregateId("123")
                .eventType("Event")
                .topic("topic")
                .payload(new byte[0])
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Aggregate type cannot be blank");
    }

    @Test
    void shouldRejectMissingTopic() {
        assertThatThrownBy(() -> OutboxEvent.builder()
                .aggregateType("Order")
                .aggregateId("123")
                .eventType("Event")
                .payload(new byte[0])
                .build())
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Topic");
    }

    @Test
    void shouldRejectNegativeRetryCount() {
        assertThatThrownBy(() -> OutboxEvent.builder()
                .aggregateType("Order")
                .aggregateId("123")
                .eventType("Event")
                .topic("topic")
                .payload(new byte[0])
                .retryCount(-1)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Retry count cannot be negative");
    }

    @Test
    void shouldReturnDefensiveCopyOfPayload() {
        byte[] original = new byte[]{1, 2, 3};
        OutboxEvent event = OutboxEvent.builder()
                .aggregateType("Order")
                .aggregateId("123")
                .eventType("Event")
                .topic("topic")
                .payload(original)
                .build();

        byte[] payload = event.payload();
        payload[0] = 99;

        assertThat(event.payload()).containsExactly(1, 2, 3);
    }

    @Test
    void shouldReturnUnmodifiableHeaders() {
        OutboxEvent event = OutboxEvent.builder()
                .aggregateType("Order")
                .aggregateId("123")
                .eventType("Event")
                .topic("topic")
                .payload(new byte[0])
                .header("key", "value")
                .build();

        assertThatThrownBy(() -> event.headers().put("new", "value"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void shouldCreateBuilderFromExistingEvent() {
        OutboxEvent original = OutboxEvent.builder()
                .aggregateType("Order")
                .aggregateId("123")
                .eventType("OrderCreated")
                .topic("orders")
                .payload(new byte[]{1, 2, 3})
                .header("h1", "v1")
                .build();

        OutboxEvent copy = original.toBuilder()
                .topic("new-topic")
                .build();

        assertThat(copy.aggregateType()).isEqualTo(original.aggregateType());
        assertThat(copy.topic()).isEqualTo("new-topic");
        assertThat(copy.headers()).isEqualTo(original.headers());
    }

    @Test
    void shouldUpdateStatus() {
        OutboxEvent pending = OutboxEvent.builder()
                .aggregateType("Order")
                .aggregateId("123")
                .eventType("Event")
                .topic("topic")
                .payload(new byte[0])
                .build();

        OutboxEvent published = pending.withStatus(Published.now());

        assertThat(pending.status()).isInstanceOf(Pending.class);
        assertThat(published.status()).isInstanceOf(Published.class);
    }

    @Test
    void shouldMarkAsPublished() {
        OutboxEvent pending = OutboxEvent.builder()
                .aggregateType("Order")
                .aggregateId("123")
                .eventType("Event")
                .topic("topic")
                .payload(new byte[0])
                .build();

        Instant publishedAt = Instant.now();
        OutboxEvent published = pending.markPublished(publishedAt);

        assertThat(published.status()).isInstanceOf(Published.class);
        assertThat(published.processedAt()).isEqualTo(publishedAt);
        assertThat(published.isPublished()).isTrue();
    }

    @Test
    void shouldMarkAsFailed() {
        OutboxEvent pending = OutboxEvent.builder()
                .aggregateType("Order")
                .aggregateId("123")
                .eventType("Event")
                .topic("topic")
                .payload(new byte[0])
                .build();

        OutboxEvent failed = pending.markFailed("Connection timeout");

        assertThat(failed.status()).isInstanceOf(Failed.class);
        assertThat(failed.retryCount()).isEqualTo(1);
        assertThat(failed.lastError()).isEqualTo("Connection timeout");
        assertThat(failed.isFailed()).isTrue();
    }

    @Test
    void shouldIncrementRetryCountOnSubsequentFailures() {
        OutboxEvent event = OutboxEvent.builder()
                .aggregateType("Order")
                .aggregateId("123")
                .eventType("Event")
                .topic("topic")
                .payload(new byte[0])
                .build();

        OutboxEvent failed1 = event.markFailed("Error 1");
        OutboxEvent failed2 = failed1.markFailed("Error 2");

        assertThat(failed2.retryCount()).isEqualTo(2);
        assertThat(failed2.lastError()).isEqualTo("Error 2");
    }

    @Test
    void shouldCheckPendingStatus() {
        OutboxEvent event = OutboxEvent.builder()
                .aggregateType("Order")
                .aggregateId("123")
                .eventType("Event")
                .topic("topic")
                .payload(new byte[0])
                .build();

        assertThat(event.isPending()).isTrue();
        assertThat(event.isPublished()).isFalse();
        assertThat(event.isFailed()).isFalse();
    }

    @Test
    void shouldHaveValueBasedEquality() {
        OutboxEventId id = OutboxEventId.generate();
        Instant now = Instant.now();

        OutboxEvent event1 = OutboxEvent.builder()
                .id(id)
                .aggregateType("Order")
                .aggregateId("123")
                .eventType("Event")
                .topic("topic")
                .payload(new byte[]{1, 2, 3})
                .createdAt(now)
                .status(Pending.at(now))
                .build();

        OutboxEvent event2 = OutboxEvent.builder()
                .id(id)
                .aggregateType("Order")
                .aggregateId("123")
                .eventType("Event")
                .topic("topic")
                .payload(new byte[]{1, 2, 3})
                .createdAt(now)
                .status(Pending.at(now))
                .build();

        assertThat(event1).isEqualTo(event2);
    }
}
