package xyz.charks.outbox.relay;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import xyz.charks.outbox.broker.BrokerConnector;
import xyz.charks.outbox.broker.PublishResult;
import xyz.charks.outbox.core.*;
import xyz.charks.outbox.retry.NoRetry;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OutboxRelayTest {

    @Mock
    private OutboxRepository repository;

    @Mock
    private BrokerConnector connector;

    private RelayConfig config;

    @BeforeEach
    void setUp() {
        config = RelayConfig.builder()
                .batchSize(10)
                .pollingInterval(Duration.ofMillis(100))
                .retryPolicy(NoRetry.instance())
                .useVirtualThreads(true)
                .build();
    }

    private OutboxEvent createEvent() {
        return OutboxEvent.builder()
                .aggregateType("Test")
                .aggregateId(AggregateId.of("123"))
                .eventType(EventType.of("TestEvent"))
                .topic("test-topic")
                .payload(new byte[]{1, 2, 3})
                .build();
    }

    @Test
    void shouldRejectNullRepository() {
        assertThatThrownBy(() -> new OutboxRelay(null, connector, config))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Repository cannot be null");
    }

    @Test
    void shouldRejectNullConnector() {
        assertThatThrownBy(() -> new OutboxRelay(repository, null, config))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Connector cannot be null");
    }

    @Test
    void shouldRejectNullConfig() {
        assertThatThrownBy(() -> new OutboxRelay(repository, connector, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Config cannot be null");
    }

    @Test
    void shouldStartAndStop() {
        OutboxRelay relay = new OutboxRelay(repository, connector, config);

        assertThat(relay.isRunning()).isFalse();

        relay.start();
        assertThat(relay.isRunning()).isTrue();

        relay.stop();
        assertThat(relay.isRunning()).isFalse();
    }

    @Test
    void shouldNotFailOnDoubleStart() {
        OutboxRelay relay = new OutboxRelay(repository, connector, config);

        relay.start();
        relay.start();

        assertThat(relay.isRunning()).isTrue();
        relay.stop();
    }

    @Test
    void shouldNotFailOnDoubleStop() {
        OutboxRelay relay = new OutboxRelay(repository, connector, config);

        relay.start();
        relay.stop();
        relay.stop();

        assertThat(relay.isRunning()).isFalse();
    }

    @Test
    void shouldProcessBatchSynchronously() {
        OutboxEvent event = createEvent();
        when(repository.find(any(OutboxQuery.class))).thenReturn(List.of(event));
        when(connector.publish(any())).thenReturn(PublishResult.success(event.id()));
        when(repository.update(any())).thenAnswer(inv -> inv.getArgument(0));

        OutboxRelay relay = new OutboxRelay(repository, connector, config);
        int processed = relay.processBatch();

        assertThat(processed).isEqualTo(1);
        verify(connector).publish(event);

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(repository).update(captor.capture());
        assertThat(captor.getValue().status()).isInstanceOf(Published.class);
    }

    @Test
    void shouldReturnZeroWhenNoEvents() {
        when(repository.find(any(OutboxQuery.class))).thenReturn(List.of());

        OutboxRelay relay = new OutboxRelay(repository, connector, config);
        int processed = relay.processBatch();

        assertThat(processed).isZero();
        verify(connector, never()).publish(any());
    }

    @Test
    void shouldHandlePublishFailure() {
        OutboxEvent event = createEvent();
        when(repository.find(any(OutboxQuery.class))).thenReturn(List.of(event));
        when(connector.publish(any())).thenReturn(PublishResult.failure(event.id(), "Connection refused"));
        when(repository.update(any())).thenAnswer(inv -> inv.getArgument(0));

        OutboxRelay relay = new OutboxRelay(repository, connector, config);
        int processed = relay.processBatch();

        assertThat(processed).isEqualTo(1);

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(repository).update(captor.capture());
        assertThat(captor.getValue().isFailed()).isTrue();
        assertThat(captor.getValue().lastError()).isEqualTo("Connection refused");
    }

    @Test
    void shouldHandlePublishException() {
        OutboxEvent event = createEvent();
        when(repository.find(any(OutboxQuery.class))).thenReturn(List.of(event));
        when(connector.publish(any())).thenThrow(new RuntimeException("Network error"));
        when(repository.update(any())).thenAnswer(inv -> inv.getArgument(0));

        OutboxRelay relay = new OutboxRelay(repository, connector, config);
        int processed = relay.processBatch();

        assertThat(processed).isEqualTo(1);

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(repository).update(captor.capture());
        assertThat(captor.getValue().isFailed()).isTrue();
        assertThat(captor.getValue().lastError()).isEqualTo("Network error");
    }

    @Test
    void shouldCallMetricsOnSuccess() {
        OutboxEvent event = createEvent();
        when(repository.find(any(OutboxQuery.class))).thenReturn(List.of(event));
        when(connector.publish(any())).thenReturn(PublishResult.success(event.id()));
        when(repository.update(any())).thenAnswer(inv -> inv.getArgument(0));

        AtomicInteger publishedCount = new AtomicInteger();
        RelayMetrics metrics = new RelayMetrics() {
            @Override
            public void onEventPublished(OutboxEvent e, Duration duration) {
                publishedCount.incrementAndGet();
            }
        };

        OutboxRelay relay = new OutboxRelay(repository, connector, config, metrics);
        relay.processBatch();

        assertThat(publishedCount.get()).isEqualTo(1);
    }

    @Test
    void shouldCallMetricsOnFailure() {
        OutboxEvent event = createEvent();
        when(repository.find(any(OutboxQuery.class))).thenReturn(List.of(event));
        when(connector.publish(any())).thenReturn(PublishResult.failure(event.id(), "Error"));
        when(repository.update(any())).thenAnswer(inv -> inv.getArgument(0));

        AtomicInteger failedCount = new AtomicInteger();
        RelayMetrics metrics = new RelayMetrics() {
            @Override
            public void onEventFailed(OutboxEvent e, Throwable error) {
                failedCount.incrementAndGet();
            }
        };

        OutboxRelay relay = new OutboxRelay(repository, connector, config, metrics);
        relay.processBatch();

        assertThat(failedCount.get()).isEqualTo(1);
    }

    @Test
    void shouldPollPeriodically() {
        when(repository.find(any(OutboxQuery.class))).thenReturn(List.of());

        RelayConfig fastConfig = RelayConfig.builder()
                .batchSize(10)
                .pollingInterval(Duration.ofMillis(50))
                .build();

        OutboxRelay relay = new OutboxRelay(repository, connector, fastConfig);
        relay.start();

        try {
            await().atMost(Duration.ofSeconds(1))
                    .untilAsserted(() -> verify(repository, times(3)).find(any(OutboxQuery.class)));
        } finally {
            relay.stop();
        }
    }
}
