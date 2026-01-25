package xyz.charks.outbox.spring;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("OutboxProperties")
class OutboxPropertiesTest {

    @Nested
    @DisplayName("default values")
    class DefaultValuesTest {

        @Test
        @DisplayName("enabled is true by default")
        void enabledByDefault() {
            OutboxProperties properties = new OutboxProperties();
            assertThat(properties.isEnabled()).isTrue();
        }

        @Test
        @DisplayName("relay has correct defaults")
        void relayDefaults() {
            OutboxProperties properties = new OutboxProperties();
            OutboxProperties.Relay relay = properties.getRelay();

            assertThat(relay.isEnabled()).isTrue();
            assertThat(relay.getPollInterval()).isEqualTo(Duration.ofSeconds(1));
            assertThat(relay.getBatchSize()).isEqualTo(100);
            assertThat(relay.getShutdownTimeout()).isEqualTo(Duration.ofSeconds(30));
        }

        @Test
        @DisplayName("retry has correct defaults")
        void retryDefaults() {
            OutboxProperties properties = new OutboxProperties();
            OutboxProperties.Retry retry = properties.getRetry();

            assertThat(retry.getMaxAttempts()).isEqualTo(3);
            assertThat(retry.getBaseDelay()).isEqualTo(Duration.ofSeconds(1));
            assertThat(retry.getMaxDelay()).isEqualTo(Duration.ofMinutes(1));
            assertThat(retry.getJitterFactor()).isEqualTo(0.1);
        }

        @Test
        @DisplayName("table has correct defaults")
        void tableDefaults() {
            OutboxProperties properties = new OutboxProperties();
            OutboxProperties.Table table = properties.getTable();

            assertThat(table.getName()).isEqualTo("outbox_events");
            assertThat(table.getSchema()).isEqualTo("public");
        }

        @Test
        @DisplayName("kafka has correct defaults")
        void kafkaDefaults() {
            OutboxProperties properties = new OutboxProperties();
            OutboxProperties.Kafka kafka = properties.getKafka();

            assertThat(kafka.getBootstrapServers()).isEqualTo("localhost:9092");
            assertThat(kafka.getAcks()).isEqualTo("all");
            assertThat(kafka.getRequestTimeout()).isEqualTo(Duration.ofSeconds(30));
        }
    }

    @Nested
    @DisplayName("setters")
    class SettersTest {

        @Test
        @DisplayName("can set enabled")
        void setEnabled() {
            OutboxProperties properties = new OutboxProperties();
            properties.setEnabled(false);
            assertThat(properties.isEnabled()).isFalse();
        }

        @Test
        @DisplayName("can set relay properties")
        void setRelayProperties() {
            OutboxProperties properties = new OutboxProperties();
            OutboxProperties.Relay relay = properties.getRelay();

            relay.setEnabled(false);
            relay.setPollInterval(Duration.ofMillis(500));
            relay.setBatchSize(50);
            relay.setShutdownTimeout(Duration.ofSeconds(10));

            assertThat(relay.isEnabled()).isFalse();
            assertThat(relay.getPollInterval()).isEqualTo(Duration.ofMillis(500));
            assertThat(relay.getBatchSize()).isEqualTo(50);
            assertThat(relay.getShutdownTimeout()).isEqualTo(Duration.ofSeconds(10));
        }

        @Test
        @DisplayName("can set retry properties")
        void setRetryProperties() {
            OutboxProperties properties = new OutboxProperties();
            OutboxProperties.Retry retry = properties.getRetry();

            retry.setMaxAttempts(5);
            retry.setBaseDelay(Duration.ofMillis(200));
            retry.setMaxDelay(Duration.ofSeconds(30));
            retry.setJitterFactor(0.2);

            assertThat(retry.getMaxAttempts()).isEqualTo(5);
            assertThat(retry.getBaseDelay()).isEqualTo(Duration.ofMillis(200));
            assertThat(retry.getMaxDelay()).isEqualTo(Duration.ofSeconds(30));
            assertThat(retry.getJitterFactor()).isEqualTo(0.2);
        }

        @Test
        @DisplayName("can set table properties")
        void setTableProperties() {
            OutboxProperties properties = new OutboxProperties();
            OutboxProperties.Table table = properties.getTable();

            table.setName("my_outbox");
            table.setSchema("custom_schema");

            assertThat(table.getName()).isEqualTo("my_outbox");
            assertThat(table.getSchema()).isEqualTo("custom_schema");
        }

        @Test
        @DisplayName("can set kafka properties")
        void setKafkaProperties() {
            OutboxProperties properties = new OutboxProperties();
            OutboxProperties.Kafka kafka = properties.getKafka();

            kafka.setBootstrapServers("kafka:9092");
            kafka.setAcks("leader");
            kafka.setRequestTimeout(Duration.ofSeconds(60));

            assertThat(kafka.getBootstrapServers()).isEqualTo("kafka:9092");
            assertThat(kafka.getAcks()).isEqualTo("leader");
            assertThat(kafka.getRequestTimeout()).isEqualTo(Duration.ofSeconds(60));
        }
    }
}
