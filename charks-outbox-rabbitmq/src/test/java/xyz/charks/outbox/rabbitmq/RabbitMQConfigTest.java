package xyz.charks.outbox.rabbitmq;

import com.rabbitmq.client.ConnectionFactory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("RabbitMQConfig")
class RabbitMQConfigTest {

    @Nested
    @DisplayName("builder")
    class BuilderTest {

        @Test
        @DisplayName("creates config with required connection factory")
        void createsWithConnectionFactory() {
            ConnectionFactory factory = new ConnectionFactory();

            RabbitMQConfig config = RabbitMQConfig.builder()
                    .connectionFactory(factory)
                    .build();

            assertThat(config.connectionFactory()).isSameAs(factory);
        }

        @Test
        @DisplayName("uses default exchange when not specified")
        void usesDefaultExchange() {
            ConnectionFactory factory = new ConnectionFactory();

            RabbitMQConfig config = RabbitMQConfig.builder()
                    .connectionFactory(factory)
                    .build();

            assertThat(config.exchange()).isEqualTo("outbox.events");
        }

        @Test
        @DisplayName("uses default routing key prefix when not specified")
        void usesDefaultRoutingKeyPrefix() {
            ConnectionFactory factory = new ConnectionFactory();

            RabbitMQConfig config = RabbitMQConfig.builder()
                    .connectionFactory(factory)
                    .build();

            assertThat(config.routingKeyPrefix()).isEqualTo("outbox.");
        }

        @Test
        @DisplayName("uses persistent delivery by default")
        void usesPersistentByDefault() {
            ConnectionFactory factory = new ConnectionFactory();

            RabbitMQConfig config = RabbitMQConfig.builder()
                    .connectionFactory(factory)
                    .build();

            assertThat(config.persistent()).isTrue();
        }

        @Test
        @DisplayName("uses non-mandatory by default")
        void usesNonMandatoryByDefault() {
            ConnectionFactory factory = new ConnectionFactory();

            RabbitMQConfig config = RabbitMQConfig.builder()
                    .connectionFactory(factory)
                    .build();

            assertThat(config.mandatory()).isFalse();
        }

        @Test
        @DisplayName("allows custom exchange")
        void customExchange() {
            ConnectionFactory factory = new ConnectionFactory();

            RabbitMQConfig config = RabbitMQConfig.builder()
                    .connectionFactory(factory)
                    .exchange("custom.exchange")
                    .build();

            assertThat(config.exchange()).isEqualTo("custom.exchange");
        }

        @Test
        @DisplayName("allows custom routing key prefix")
        void customRoutingKeyPrefix() {
            ConnectionFactory factory = new ConnectionFactory();

            RabbitMQConfig config = RabbitMQConfig.builder()
                    .connectionFactory(factory)
                    .routingKeyPrefix("events.")
                    .build();

            assertThat(config.routingKeyPrefix()).isEqualTo("events.");
        }

        @Test
        @DisplayName("allows disabling persistence")
        void disablePersistence() {
            ConnectionFactory factory = new ConnectionFactory();

            RabbitMQConfig config = RabbitMQConfig.builder()
                    .connectionFactory(factory)
                    .persistent(false)
                    .build();

            assertThat(config.persistent()).isFalse();
        }

        @Test
        @DisplayName("allows enabling mandatory flag")
        void enableMandatory() {
            ConnectionFactory factory = new ConnectionFactory();

            RabbitMQConfig config = RabbitMQConfig.builder()
                    .connectionFactory(factory)
                    .mandatory(true)
                    .build();

            assertThat(config.mandatory()).isTrue();
        }

        @Test
        @DisplayName("throws exception when connection factory is null")
        void nullConnectionFactory() {
            assertThatThrownBy(() -> RabbitMQConfig.builder().build())
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("connectionFactory");
        }
    }
}
