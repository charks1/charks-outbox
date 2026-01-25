package xyz.charks.outbox.test;

import xyz.charks.outbox.broker.PublishResult;
import xyz.charks.outbox.core.AggregateId;
import xyz.charks.outbox.core.EventType;
import xyz.charks.outbox.core.OutboxEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("MockBrokerConnector")
class MockBrokerConnectorTest {

    private MockBrokerConnector connector;

    @BeforeEach
    void setup() {
        connector = new MockBrokerConnector();
    }

    @Nested
    @DisplayName("publish")
    class PublishTest {

        @Test
        @DisplayName("returns success by default")
        void returnsSuccessByDefault() {
            OutboxEvent event = createTestEvent();

            PublishResult result = connector.publish(event);

            assertThat(result.success()).isTrue();
            assertThat(result.eventId()).isEqualTo(event.id());
        }

        @Test
        @DisplayName("records published event")
        void recordsPublishedEvent() {
            OutboxEvent event = createTestEvent();

            connector.publish(event);

            assertThat(connector.publishedEvents()).hasSize(1);
            assertThat(connector.publishedEvents().get(0)).isEqualTo(event);
        }

        @Test
        @DisplayName("increments offset for each publish")
        void incrementsOffset() {
            OutboxEvent e1 = createTestEvent();
            OutboxEvent e2 = createTestEvent();

            PublishResult r1 = connector.publish(e1);
            PublishResult r2 = connector.publish(e2);

            assertThat(r1.offset()).isLessThan(r2.offset());
        }

        @Test
        @DisplayName("rejects null event")
        void rejectsNullEvent() {
            assertThatThrownBy(() -> connector.publish(null))
                    .isInstanceOf(NullPointerException.class);
        }
    }

    @Nested
    @DisplayName("failure configuration")
    class FailureConfigurationTest {

        @Test
        @DisplayName("failNextPublish causes single failure")
        void failNextPublishCausesSingleFailure() {
            connector.failNextPublish("Simulated failure");

            PublishResult r1 = connector.publish(createTestEvent());
            PublishResult r2 = connector.publish(createTestEvent());

            assertThat(r1.success()).isFalse();
            assertThat(r1.error()).isEqualTo("Simulated failure");
            assertThat(r2.success()).isTrue();
        }

        @Test
        @DisplayName("failNextPublishes causes multiple failures")
        void failNextPublishesCausesMultipleFailures() {
            connector.failNextPublishes(2, "Error");

            PublishResult r1 = connector.publish(createTestEvent());
            PublishResult r2 = connector.publish(createTestEvent());
            PublishResult r3 = connector.publish(createTestEvent());

            assertThat(r1.success()).isFalse();
            assertThat(r2.success()).isFalse();
            assertThat(r3.success()).isTrue();
        }

        @Test
        @DisplayName("failAllPublishes causes all to fail")
        void failAllPublishesCausesAllToFail() {
            connector.failAllPublishes("Broker down");

            PublishResult r1 = connector.publish(createTestEvent());
            PublishResult r2 = connector.publish(createTestEvent());

            assertThat(r1.success()).isFalse();
            assertThat(r2.success()).isFalse();
        }

        @Test
        @DisplayName("succeedAllPublishes resets to success")
        void succeedAllPublishesResetsToSuccess() {
            connector.failAllPublishes("Error");
            connector.succeedAllPublishes();

            PublishResult result = connector.publish(createTestEvent());

            assertThat(result.success()).isTrue();
        }
    }

    @Nested
    @DisplayName("health")
    class HealthTest {

        @Test
        @DisplayName("isHealthy returns true by default")
        void isHealthyByDefault() {
            assertThat(connector.isHealthy()).isTrue();
        }

        @Test
        @DisplayName("setHealthy controls health status")
        void setHealthyControlsStatus() {
            connector.setHealthy(false);

            assertThat(connector.isHealthy()).isFalse();

            connector.setHealthy(true);

            assertThat(connector.isHealthy()).isTrue();
        }
    }

    @Nested
    @DisplayName("query methods")
    class QueryMethodsTest {

        @Test
        @DisplayName("publishCount returns count")
        void publishCountReturnsCount() {
            connector.publish(createTestEvent());
            connector.publish(createTestEvent());

            assertThat(connector.publishCount()).isEqualTo(2);
        }

        @Test
        @DisplayName("lastPublished returns last event")
        void lastPublishedReturnsLast() {
            OutboxEvent e1 = createTestEvent();
            OutboxEvent e2 = createTestEvent();

            connector.publish(e1);
            connector.publish(e2);

            assertThat(connector.lastPublished()).isEqualTo(e2);
        }

        @Test
        @DisplayName("lastPublished returns null when empty")
        void lastPublishedReturnsNullWhenEmpty() {
            assertThat(connector.lastPublished()).isNull();
        }

        @Test
        @DisplayName("wasPublished checks if event was published")
        void wasPublishedChecks() {
            OutboxEvent event = createTestEvent();

            assertThat(connector.wasPublished(event)).isFalse();

            connector.publish(event);

            assertThat(connector.wasPublished(event)).isTrue();
        }

        @Test
        @DisplayName("eventsForTopic filters by topic")
        void eventsForTopicFilters() {
            OutboxEvent ordersEvent = createTestEvent("orders");
            OutboxEvent paymentsEvent = createTestEvent("payments");

            connector.publish(ordersEvent);
            connector.publish(paymentsEvent);

            assertThat(connector.eventsForTopic("orders")).hasSize(1);
            assertThat(connector.eventsForTopic("orders").get(0).topic()).isEqualTo("orders");
        }
    }

    @Nested
    @DisplayName("reset")
    class ResetTest {

        @Test
        @DisplayName("clears all state")
        void clearsAllState() {
            connector.publish(createTestEvent());
            connector.failAllPublishes("Error");
            connector.setHealthy(false);

            connector.reset();

            assertThat(connector.publishedEvents()).isEmpty();
            assertThat(connector.isHealthy()).isTrue();
            assertThat(connector.publish(createTestEvent()).success()).isTrue();
        }
    }

    @Nested
    @DisplayName("custom behavior")
    class CustomBehaviorTest {

        @Test
        @DisplayName("setPublishBehavior applies custom logic")
        void customBehavior() {
            connector.setPublishBehavior(event ->
                    event.topic().equals("orders")
                            ? PublishResult.success(event.id())
                            : PublishResult.failure(event.id(), "Wrong topic")
            );

            OutboxEvent ordersEvent = createTestEvent("orders");
            OutboxEvent otherEvent = createTestEvent("other");

            assertThat(connector.publish(ordersEvent).success()).isTrue();
            assertThat(connector.publish(otherEvent).success()).isFalse();
        }
    }

    private OutboxEvent createTestEvent() {
        return createTestEvent("orders");
    }

    private OutboxEvent createTestEvent(String topic) {
        return OutboxEvent.builder()
                .aggregateType("Order")
                .aggregateId(AggregateId.of("order-" + System.nanoTime()))
                .eventType(EventType.of("OrderCreated"))
                .topic(topic)
                .payload("{\"test\":true}".getBytes(StandardCharsets.UTF_8))
                .build();
    }
}
