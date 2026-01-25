package xyz.charks.outbox.pulsar;

import org.apache.pulsar.client.api.PulsarClient;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

@DisplayName("PulsarConfig")
class PulsarConfigTest {

    @Nested
    @DisplayName("builder")
    class BuilderTest {

        @Test
        @DisplayName("creates config with required client")
        void createsWithClient() {
            PulsarClient client = mock(PulsarClient.class);

            PulsarConfig config = PulsarConfig.builder()
                    .client(client)
                    .build();

            assertThat(config.client()).isSameAs(client);
        }

        @Test
        @DisplayName("uses default topic prefix when not specified")
        void usesDefaultTopicPrefix() {
            PulsarClient client = mock(PulsarClient.class);

            PulsarConfig config = PulsarConfig.builder()
                    .client(client)
                    .build();

            assertThat(config.topicPrefix()).isEqualTo("persistent://public/default/outbox-");
        }

        @Test
        @DisplayName("enables batching by default")
        void enablesBatchingByDefault() {
            PulsarClient client = mock(PulsarClient.class);

            PulsarConfig config = PulsarConfig.builder()
                    .client(client)
                    .build();

            assertThat(config.enableBatching()).isTrue();
        }

        @Test
        @DisplayName("uses default batching max messages")
        void usesDefaultBatchingMaxMessages() {
            PulsarClient client = mock(PulsarClient.class);

            PulsarConfig config = PulsarConfig.builder()
                    .client(client)
                    .build();

            assertThat(config.batchingMaxMessages()).isEqualTo(1000);
        }

        @Test
        @DisplayName("allows custom topic prefix")
        void customTopicPrefix() {
            PulsarClient client = mock(PulsarClient.class);

            PulsarConfig config = PulsarConfig.builder()
                    .client(client)
                    .topicPrefix("persistent://tenant/namespace/events-")
                    .build();

            assertThat(config.topicPrefix()).isEqualTo("persistent://tenant/namespace/events-");
        }

        @Test
        @DisplayName("allows disabling batching")
        void disableBatching() {
            PulsarClient client = mock(PulsarClient.class);

            PulsarConfig config = PulsarConfig.builder()
                    .client(client)
                    .enableBatching(false)
                    .build();

            assertThat(config.enableBatching()).isFalse();
        }

        @Test
        @DisplayName("allows custom batching max messages")
        void customBatchingMaxMessages() {
            PulsarClient client = mock(PulsarClient.class);

            PulsarConfig config = PulsarConfig.builder()
                    .client(client)
                    .batchingMaxMessages(500)
                    .build();

            assertThat(config.batchingMaxMessages()).isEqualTo(500);
        }

        @Test
        @DisplayName("throws exception when client is null")
        void nullClient() {
            assertThatThrownBy(() -> PulsarConfig.builder().build())
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("client");
        }
    }
}
