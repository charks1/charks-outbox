package xyz.charks.outbox.kafka;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("KafkaConfig")
class KafkaConfigTest {

    @Nested
    @DisplayName("builder")
    class BuilderTest {

        @Test
        @DisplayName("creates config with default values")
        void defaultValues() {
            KafkaConfig config = KafkaConfig.builder().build();

            assertThat(config.bootstrapServers()).isEqualTo("localhost:9092");
            assertThat(config.acks()).isEqualTo(KafkaConfig.Acks.ALL);
            assertThat(config.requestTimeout()).isEqualTo(Duration.ofSeconds(30));
            assertThat(config.retries()).isEqualTo(3);
            assertThat(config.lingerMs()).isEqualTo(5);
            assertThat(config.batchSize()).isEqualTo(16384);
            assertThat(config.bufferMemory()).isEqualTo(33554432L);
            assertThat(config.compressionType()).isEqualTo("none");
            assertThat(config.additionalProperties()).isEmpty();
        }

        @Test
        @DisplayName("creates config with custom values")
        void customValues() {
            KafkaConfig config = KafkaConfig.builder()
                    .bootstrapServers("broker1:9092,broker2:9092")
                    .acks(KafkaConfig.Acks.LEADER)
                    .requestTimeout(Duration.ofSeconds(60))
                    .retries(5)
                    .lingerMs(10)
                    .batchSize(32768)
                    .bufferMemory(67108864L)
                    .compressionType("gzip")
                    .property("max.block.ms", 5000)
                    .build();

            assertThat(config.bootstrapServers()).isEqualTo("broker1:9092,broker2:9092");
            assertThat(config.acks()).isEqualTo(KafkaConfig.Acks.LEADER);
            assertThat(config.requestTimeout()).isEqualTo(Duration.ofSeconds(60));
            assertThat(config.retries()).isEqualTo(5);
            assertThat(config.lingerMs()).isEqualTo(10);
            assertThat(config.batchSize()).isEqualTo(32768);
            assertThat(config.bufferMemory()).isEqualTo(67108864L);
            assertThat(config.compressionType()).isEqualTo("gzip");
            assertThat(config.additionalProperties()).containsEntry("max.block.ms", 5000);
        }

        @Test
        @DisplayName("adds multiple properties at once")
        void multipleProperties() {
            Map<String, Object> props = Map.of(
                    "max.block.ms", 5000,
                    "enable.idempotence", true
            );

            KafkaConfig config = KafkaConfig.builder()
                    .properties(props)
                    .build();

            assertThat(config.additionalProperties())
                    .containsEntry("max.block.ms", 5000)
                    .containsEntry("enable.idempotence", true);
        }
    }

    @Nested
    @DisplayName("validation")
    class ValidationTest {

        @Test
        @DisplayName("rejects null bootstrap servers")
        void nullBootstrapServers() {
            assertThatThrownBy(() -> KafkaConfig.builder()
                    .bootstrapServers(null)
                    .build())
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("Bootstrap servers");
        }

        @Test
        @DisplayName("rejects blank bootstrap servers")
        void blankBootstrapServers() {
            assertThatThrownBy(() -> KafkaConfig.builder()
                    .bootstrapServers("  ")
                    .build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("blank");
        }

        @Test
        @DisplayName("rejects negative retries")
        void negativeRetries() {
            assertThatThrownBy(() -> KafkaConfig.builder()
                    .retries(-1)
                    .build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("negative");
        }
    }

    @Nested
    @DisplayName("localhost")
    class LocalhostTest {

        @Test
        @DisplayName("creates localhost configuration")
        void localhostConfig() {
            KafkaConfig config = KafkaConfig.localhost();

            assertThat(config.bootstrapServers()).isEqualTo("localhost:9092");
            assertThat(config.acks()).isEqualTo(KafkaConfig.Acks.ALL);
        }
    }

    @Nested
    @DisplayName("toProducerProperties")
    class ToProducerPropertiesTest {

        @Test
        @DisplayName("converts to Kafka producer properties")
        void convertsToProperties() {
            KafkaConfig config = KafkaConfig.builder()
                    .bootstrapServers("broker:9092")
                    .acks(KafkaConfig.Acks.ALL)
                    .requestTimeout(Duration.ofSeconds(45))
                    .retries(5)
                    .property("custom.property", "value")
                    .build();

            Map<String, Object> props = config.toProducerProperties();

            assertThat(props)
                    .containsEntry("bootstrap.servers", "broker:9092")
                    .containsEntry("acks", "all")
                    .containsEntry("request.timeout.ms", 45000)
                    .containsEntry("retries", 5)
                    .containsEntry("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
                    .containsEntry("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
                    .containsEntry("custom.property", "value");
        }
    }

    @Nested
    @DisplayName("Acks")
    class AcksTest {

        @Test
        @DisplayName("returns correct values")
        void acksValues() {
            assertThat(KafkaConfig.Acks.NONE.value()).isEqualTo("0");
            assertThat(KafkaConfig.Acks.LEADER.value()).isEqualTo("1");
            assertThat(KafkaConfig.Acks.ALL.value()).isEqualTo("all");
        }
    }

    @Nested
    @DisplayName("immutability")
    class ImmutabilityTest {

        @Test
        @DisplayName("additional properties are immutable")
        void additionalPropertiesImmutable() {
            KafkaConfig config = KafkaConfig.builder()
                    .property("key", "value")
                    .build();

            assertThatThrownBy(() -> config.additionalProperties().put("new", "value"))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }
}
