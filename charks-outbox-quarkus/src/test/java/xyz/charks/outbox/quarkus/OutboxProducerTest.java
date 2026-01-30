package xyz.charks.outbox.quarkus;

import jakarta.enterprise.inject.Instance;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import xyz.charks.outbox.broker.BrokerConnector;
import xyz.charks.outbox.core.OutboxRepository;
import xyz.charks.outbox.relay.OutboxRelay;
import xyz.charks.outbox.relay.RelayConfig;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@DisplayName("OutboxProducer")
class OutboxProducerTest {

    private OutboxProducer producer;
    private OutboxConfiguration config;
    private OutboxConfiguration.RelayConfiguration relayConfig;
    private OutboxConfiguration.RetryConfiguration retryConfig;
    private Instance<OutboxRepository> repositoryInstance;
    private Instance<BrokerConnector> connectorInstance;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        config = mock(OutboxConfiguration.class);
        relayConfig = mock(OutboxConfiguration.RelayConfiguration.class);
        retryConfig = mock(OutboxConfiguration.RetryConfiguration.class);
        repositoryInstance = mock(Instance.class);
        connectorInstance = mock(Instance.class);

        when(config.relay()).thenReturn(relayConfig);
        when(config.retry()).thenReturn(retryConfig);

        when(relayConfig.batchSize()).thenReturn(100);
        when(relayConfig.pollingInterval()).thenReturn(Duration.ofSeconds(1));
        when(relayConfig.shutdownTimeout()).thenReturn(Duration.ofSeconds(30));

        when(retryConfig.maxAttempts()).thenReturn(10);
        when(retryConfig.initialDelay()).thenReturn(Duration.ofSeconds(1));
        when(retryConfig.maxDelay()).thenReturn(Duration.ofMinutes(5));
        when(retryConfig.multiplier()).thenReturn(2.0);

        producer = new OutboxProducer(config, repositoryInstance, connectorInstance);
    }

    @Nested
    @DisplayName("relayConfig")
    class RelayConfigTest {

        @Test
        @DisplayName("creates config from Quarkus configuration")
        void createsConfigFromQuarkusConfig() {
            RelayConfig result = producer.relayConfig();

            assertThat(result.batchSize()).isEqualTo(100);
            assertThat(result.pollingInterval()).isEqualTo(Duration.ofSeconds(1));
            assertThat(result.shutdownTimeout()).isEqualTo(Duration.ofSeconds(30));
            assertThat(result.retryPolicy()).isNotNull();
        }

        @Test
        @DisplayName("uses retry configuration")
        void usesRetryConfiguration() {
            when(retryConfig.maxAttempts()).thenReturn(5);
            when(retryConfig.initialDelay()).thenReturn(Duration.ofMillis(500));

            RelayConfig result = producer.relayConfig();

            assertThat(result.retryPolicy()).isNotNull();
        }
    }

    @Nested
    @DisplayName("outboxRelay")
    class OutboxRelayTest {

        @Test
        @DisplayName("returns null when disabled")
        void returnsNullWhenDisabled() {
            when(config.enabled()).thenReturn(false);

            OutboxRelay result = producer.outboxRelay(producer.relayConfig());

            assertThat(result).isNull();
        }

        @Test
        @DisplayName("returns null when repository is not available")
        void returnsNullWhenRepositoryNotAvailable() {
            when(config.enabled()).thenReturn(true);
            when(repositoryInstance.isUnsatisfied()).thenReturn(true);

            OutboxRelay result = producer.outboxRelay(producer.relayConfig());

            assertThat(result).isNull();
        }

        @Test
        @DisplayName("returns null when connector is not available")
        void returnsNullWhenConnectorNotAvailable() {
            when(config.enabled()).thenReturn(true);
            when(repositoryInstance.isUnsatisfied()).thenReturn(false);
            when(connectorInstance.isUnsatisfied()).thenReturn(true);

            OutboxRelay result = producer.outboxRelay(producer.relayConfig());

            assertThat(result).isNull();
        }

        @Test
        @DisplayName("creates relay when all dependencies available")
        void createsRelayWhenAllDependenciesAvailable() {
            when(config.enabled()).thenReturn(true);
            when(repositoryInstance.isUnsatisfied()).thenReturn(false);
            when(connectorInstance.isUnsatisfied()).thenReturn(false);

            OutboxRepository repository = mock(OutboxRepository.class);
            BrokerConnector connector = mock(BrokerConnector.class);
            when(repositoryInstance.get()).thenReturn(repository);
            when(connectorInstance.get()).thenReturn(connector);

            OutboxRelay result = producer.outboxRelay(producer.relayConfig());

            assertThat(result).isNotNull();
        }
    }

}
