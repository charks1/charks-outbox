package xyz.charks.outbox.spring;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import xyz.charks.outbox.retry.ExponentialBackoff;
import xyz.charks.outbox.retry.RetryPolicy;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("OutboxAutoConfiguration")
class OutboxAutoConfigurationTest {

    @Nested
    @DisplayName("outboxRetryPolicy")
    class RetryPolicyBeanTest {

        @Test
        @DisplayName("creates ExponentialBackoff with default properties")
        void createsRetryPolicyWithDefaults() {
            OutboxAutoConfiguration config = new OutboxAutoConfiguration();
            OutboxProperties properties = new OutboxProperties();

            RetryPolicy retryPolicy = config.outboxRetryPolicy(properties);

            assertThat(retryPolicy).isInstanceOf(ExponentialBackoff.class);
        }

        @Test
        @DisplayName("creates ExponentialBackoff with custom properties")
        void createsRetryPolicyWithCustomProperties() {
            OutboxAutoConfiguration config = new OutboxAutoConfiguration();
            OutboxProperties properties = new OutboxProperties();
            properties.getRetry().setMaxAttempts(5);
            properties.getRetry().setBaseDelay(Duration.ofMillis(500));
            properties.getRetry().setMaxDelay(Duration.ofSeconds(30));
            properties.getRetry().setJitterFactor(0.15);

            RetryPolicy retryPolicy = config.outboxRetryPolicy(properties);

            assertThat(retryPolicy).isInstanceOf(ExponentialBackoff.class);
            ExponentialBackoff backoff = (ExponentialBackoff) retryPolicy;
            assertThat(backoff.maxAttempts()).isEqualTo(5);
        }
    }

    @Nested
    @DisplayName("JsonSerializerAutoConfiguration")
    class JsonSerializerTest {

        @Test
        @DisplayName("creates JacksonSerializer")
        void createsJacksonSerializer() {
            var jsonConfig = new OutboxAutoConfiguration.JsonSerializerAutoConfiguration();

            var serializer = jsonConfig.outboxSerializer();

            assertThat(serializer).isNotNull();
            assertThat(serializer.contentType()).isEqualTo("application/json");
        }
    }

    @Nested
    @DisplayName("RelayAutoConfiguration")
    class RelayConfigTest {

        @Test
        @DisplayName("creates RelayConfig with default properties")
        void createsRelayConfigWithDefaults() {
            var relayConfig = new OutboxAutoConfiguration.RelayAutoConfiguration();
            OutboxProperties properties = new OutboxProperties();
            RetryPolicy retryPolicy = new OutboxAutoConfiguration().outboxRetryPolicy(properties);

            var config = relayConfig.outboxRelayConfig(properties, retryPolicy);

            assertThat(config.batchSize()).isEqualTo(100);
            assertThat(config.pollingInterval()).isEqualTo(Duration.ofSeconds(1));
            assertThat(config.shutdownTimeout()).isEqualTo(Duration.ofSeconds(30));
            assertThat(config.retryPolicy()).isSameAs(retryPolicy);
        }

        @Test
        @DisplayName("creates RelayConfig with custom properties")
        void createsRelayConfigWithCustomProperties() {
            var relayConfig = new OutboxAutoConfiguration.RelayAutoConfiguration();
            OutboxProperties properties = new OutboxProperties();
            properties.getRelay().setBatchSize(50);
            properties.getRelay().setPollInterval(Duration.ofMillis(500));
            properties.getRelay().setShutdownTimeout(Duration.ofSeconds(15));
            RetryPolicy retryPolicy = new OutboxAutoConfiguration().outboxRetryPolicy(properties);

            var config = relayConfig.outboxRelayConfig(properties, retryPolicy);

            assertThat(config.batchSize()).isEqualTo(50);
            assertThat(config.pollingInterval()).isEqualTo(Duration.ofMillis(500));
            assertThat(config.shutdownTimeout()).isEqualTo(Duration.ofSeconds(15));
        }
    }
}
