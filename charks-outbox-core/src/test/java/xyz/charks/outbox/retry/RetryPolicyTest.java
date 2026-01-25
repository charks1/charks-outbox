package xyz.charks.outbox.retry;

import xyz.charks.outbox.core.AggregateId;
import xyz.charks.outbox.core.EventType;
import xyz.charks.outbox.core.OutboxEvent;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RetryPolicyTest {

    private static OutboxEvent createEventWithRetryCount(int retryCount) {
        return OutboxEvent.builder()
                .aggregateType("Test")
                .aggregateId(AggregateId.of("1"))
                .eventType(EventType.of("TestEvent"))
                .topic("test")
                .payload(new byte[0])
                .retryCount(retryCount)
                .build();
    }

    @Nested
    class ExponentialBackoffTest {

        @Test
        void shouldCreateWithDefaults() {
            ExponentialBackoff policy = ExponentialBackoff.defaults();

            assertThat(policy.maxAttempts()).isEqualTo(3);
            assertThat(policy.baseDelay()).isEqualTo(Duration.ofSeconds(1));
            assertThat(policy.maxDelay()).isEqualTo(Duration.ofMinutes(1));
            assertThat(policy.jitterFactor()).isEqualTo(0.1);
        }

        @Test
        void shouldBuildWithCustomValues() {
            ExponentialBackoff policy = ExponentialBackoff.builder()
                    .maxAttempts(5)
                    .baseDelay(Duration.ofMillis(500))
                    .maxDelay(Duration.ofSeconds(30))
                    .jitterFactor(0.2)
                    .build();

            assertThat(policy.maxAttempts()).isEqualTo(5);
            assertThat(policy.baseDelay()).isEqualTo(Duration.ofMillis(500));
            assertThat(policy.maxDelay()).isEqualTo(Duration.ofSeconds(30));
            assertThat(policy.jitterFactor()).isEqualTo(0.2);
        }

        @Test
        void shouldCalculateExponentialDelay() {
            ExponentialBackoff policy = ExponentialBackoff.builder()
                    .baseDelay(Duration.ofSeconds(1))
                    .maxDelay(Duration.ofMinutes(10))
                    .jitterFactor(0)
                    .maxAttempts(10)
                    .build();

            OutboxEvent event0 = createEventWithRetryCount(0);
            OutboxEvent event1 = createEventWithRetryCount(1);
            OutboxEvent event2 = createEventWithRetryCount(2);
            OutboxEvent event3 = createEventWithRetryCount(3);

            RuntimeException error = new RuntimeException("test");

            assertThat(policy.nextRetryDelay(event0, error)).hasValue(Duration.ofSeconds(1));
            assertThat(policy.nextRetryDelay(event1, error)).hasValue(Duration.ofSeconds(2));
            assertThat(policy.nextRetryDelay(event2, error)).hasValue(Duration.ofSeconds(4));
            assertThat(policy.nextRetryDelay(event3, error)).hasValue(Duration.ofSeconds(8));
        }

        @Test
        void shouldCapDelayAtMaximum() {
            ExponentialBackoff policy = ExponentialBackoff.builder()
                    .baseDelay(Duration.ofSeconds(10))
                    .maxDelay(Duration.ofSeconds(30))
                    .jitterFactor(0)
                    .maxAttempts(10)
                    .build();

            OutboxEvent event = createEventWithRetryCount(5);
            RuntimeException error = new RuntimeException("test");

            assertThat(policy.nextRetryDelay(event, error)).hasValue(Duration.ofSeconds(30));
        }

        @Test
        void shouldAddJitter() {
            ExponentialBackoff policy = ExponentialBackoff.builder()
                    .baseDelay(Duration.ofSeconds(1))
                    .jitterFactor(0.5)
                    .maxAttempts(10)
                    .build();

            OutboxEvent event = createEventWithRetryCount(0);
            RuntimeException error = new RuntimeException("test");

            Duration delay = policy.nextRetryDelay(event, error).orElseThrow();
            assertThat(delay).isBetween(Duration.ofMillis(1000), Duration.ofMillis(1500));
        }

        @Test
        void shouldReturnEmptyWhenMaxAttemptsExceeded() {
            ExponentialBackoff policy = ExponentialBackoff.builder()
                    .maxAttempts(3)
                    .build();

            OutboxEvent event = createEventWithRetryCount(3);
            RuntimeException error = new RuntimeException("test");

            assertThat(policy.nextRetryDelay(event, error)).isEmpty();
            assertThat(policy.shouldRetry(event)).isFalse();
        }

        @Test
        void shouldRejectInvalidMaxAttempts() {
            assertThatThrownBy(() -> ExponentialBackoff.builder().maxAttempts(0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Max attempts must be positive");
        }

        @Test
        void shouldRejectNegativeBaseDelay() {
            assertThatThrownBy(() -> ExponentialBackoff.builder().baseDelay(Duration.ofSeconds(-1)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Base delay cannot be negative");
        }

        @Test
        void shouldRejectInvalidJitterFactor() {
            assertThatThrownBy(() -> ExponentialBackoff.builder().jitterFactor(1.5))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Jitter factor must be between 0 and 1");
        }
    }

    @Nested
    class FixedDelayTest {

        @Test
        void shouldCreateWithDelayAndMaxAttempts() {
            FixedDelay policy = FixedDelay.of(Duration.ofSeconds(5), 3);

            assertThat(policy.delay()).isEqualTo(Duration.ofSeconds(5));
            assertThat(policy.maxAttempts()).isEqualTo(3);
        }

        @Test
        void shouldCreateWithDelayOnly() {
            FixedDelay policy = FixedDelay.withDelay(Duration.ofSeconds(10));

            assertThat(policy.delay()).isEqualTo(Duration.ofSeconds(10));
            assertThat(policy.maxAttempts()).isEqualTo(3);
        }

        @Test
        void shouldReturnConstantDelay() {
            FixedDelay policy = FixedDelay.of(Duration.ofSeconds(5), 5);

            OutboxEvent event0 = createEventWithRetryCount(0);
            OutboxEvent event1 = createEventWithRetryCount(1);
            OutboxEvent event2 = createEventWithRetryCount(2);
            RuntimeException error = new RuntimeException("test");

            assertThat(policy.nextRetryDelay(event0, error)).hasValue(Duration.ofSeconds(5));
            assertThat(policy.nextRetryDelay(event1, error)).hasValue(Duration.ofSeconds(5));
            assertThat(policy.nextRetryDelay(event2, error)).hasValue(Duration.ofSeconds(5));
        }

        @Test
        void shouldReturnEmptyWhenMaxAttemptsExceeded() {
            FixedDelay policy = FixedDelay.of(Duration.ofSeconds(5), 2);

            OutboxEvent event = createEventWithRetryCount(2);
            RuntimeException error = new RuntimeException("test");

            assertThat(policy.nextRetryDelay(event, error)).isEmpty();
        }

        @Test
        void shouldRejectNullDelay() {
            assertThatThrownBy(() -> FixedDelay.of(null, 3))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("Delay cannot be null");
        }

        @Test
        void shouldRejectNegativeDelay() {
            assertThatThrownBy(() -> FixedDelay.of(Duration.ofSeconds(-1), 3))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Delay cannot be negative");
        }

        @Test
        void shouldRejectInvalidMaxAttempts() {
            assertThatThrownBy(() -> FixedDelay.of(Duration.ofSeconds(1), 0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Max attempts must be positive");
        }
    }

    @Nested
    class NoRetryTest {

        @Test
        void shouldNeverRetry() {
            NoRetry policy = NoRetry.instance();

            OutboxEvent event = createEventWithRetryCount(0);
            RuntimeException error = new RuntimeException("test");

            assertThat(policy.nextRetryDelay(event, error)).isEmpty();
            assertThat(policy.shouldRetry(event)).isFalse();
        }

        @Test
        void shouldHaveSingleAttempt() {
            NoRetry policy = NoRetry.instance();

            assertThat(policy.maxAttempts()).isEqualTo(1);
        }

        @Test
        void shouldBeSingleton() {
            assertThat(NoRetry.instance()).isSameAs(NoRetry.instance());
        }
    }
}
