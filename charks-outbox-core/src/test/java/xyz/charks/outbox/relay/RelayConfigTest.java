package xyz.charks.outbox.relay;

import xyz.charks.outbox.core.LockMode;
import xyz.charks.outbox.retry.ExponentialBackoff;
import xyz.charks.outbox.retry.FixedDelay;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RelayConfigTest {

    @Test
    void shouldCreateWithDefaults() {
        RelayConfig config = RelayConfig.defaults();

        assertThat(config.batchSize()).isEqualTo(100);
        assertThat(config.pollingInterval()).isEqualTo(Duration.ofSeconds(1));
        assertThat(config.lockMode()).isEqualTo(LockMode.FOR_UPDATE_SKIP_LOCKED);
        assertThat(config.retryPolicy()).isInstanceOf(ExponentialBackoff.class);
        assertThat(config.shutdownTimeout()).isEqualTo(Duration.ofSeconds(30));
        assertThat(config.useVirtualThreads()).isTrue();
    }

    @Test
    void shouldBuildWithCustomValues() {
        FixedDelay retryPolicy = FixedDelay.of(Duration.ofSeconds(5), 5);

        RelayConfig config = RelayConfig.builder()
                .batchSize(50)
                .pollingInterval(Duration.ofMillis(500))
                .lockMode(LockMode.FOR_UPDATE)
                .retryPolicy(retryPolicy)
                .shutdownTimeout(Duration.ofMinutes(1))
                .useVirtualThreads(false)
                .build();

        assertThat(config.batchSize()).isEqualTo(50);
        assertThat(config.pollingInterval()).isEqualTo(Duration.ofMillis(500));
        assertThat(config.lockMode()).isEqualTo(LockMode.FOR_UPDATE);
        assertThat(config.retryPolicy()).isSameAs(retryPolicy);
        assertThat(config.shutdownTimeout()).isEqualTo(Duration.ofMinutes(1));
        assertThat(config.useVirtualThreads()).isFalse();
    }

    @Test
    void shouldRejectNonPositiveBatchSize() {
        assertThatThrownBy(() -> RelayConfig.builder().batchSize(0).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Batch size must be positive");

        assertThatThrownBy(() -> RelayConfig.builder().batchSize(-1).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Batch size must be positive");
    }

    @Test
    void shouldRejectNonPositivePollingInterval() {
        assertThatThrownBy(() -> RelayConfig.builder().pollingInterval(Duration.ZERO).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Polling interval must be positive");

        assertThatThrownBy(() -> RelayConfig.builder().pollingInterval(Duration.ofSeconds(-1)).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Polling interval must be positive");
    }

    @Test
    void shouldRejectNullPollingInterval() {
        assertThatThrownBy(() -> RelayConfig.builder().pollingInterval(null).build())
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Polling interval cannot be null");
    }

    @Test
    void shouldRejectNullLockMode() {
        assertThatThrownBy(() -> RelayConfig.builder().lockMode(null).build())
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Lock mode cannot be null");
    }

    @Test
    void shouldRejectNullRetryPolicy() {
        assertThatThrownBy(() -> RelayConfig.builder().retryPolicy(null).build())
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Retry policy cannot be null");
    }

    @Test
    void shouldRejectNegativeShutdownTimeout() {
        assertThatThrownBy(() -> RelayConfig.builder().shutdownTimeout(Duration.ofSeconds(-1)).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Shutdown timeout cannot be negative");
    }

    @Test
    void shouldAllowZeroShutdownTimeout() {
        RelayConfig config = RelayConfig.builder()
                .shutdownTimeout(Duration.ZERO)
                .build();

        assertThat(config.shutdownTimeout()).isEqualTo(Duration.ZERO);
    }

    @Test
    void shouldHaveValueBasedEquality() {
        RelayConfig config1 = RelayConfig.builder()
                .batchSize(50)
                .pollingInterval(Duration.ofMillis(500))
                .build();

        RelayConfig config2 = RelayConfig.builder()
                .batchSize(50)
                .pollingInterval(Duration.ofMillis(500))
                .build();

        assertThat(config1).isEqualTo(config2);
    }
}
