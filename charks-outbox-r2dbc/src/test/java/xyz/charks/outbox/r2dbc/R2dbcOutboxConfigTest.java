package xyz.charks.outbox.r2dbc;

import io.r2dbc.spi.ConnectionFactory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

@DisplayName("R2dbcOutboxConfig")
class R2dbcOutboxConfigTest {

    @Nested
    @DisplayName("builder")
    class BuilderTest {

        @Test
        @DisplayName("creates config with required connection factory")
        void createsWithConnectionFactory() {
            ConnectionFactory connectionFactory = mock(ConnectionFactory.class);

            R2dbcOutboxConfig config = R2dbcOutboxConfig.builder()
                    .connectionFactory(connectionFactory)
                    .build();

            assertThat(config.connectionFactory()).isSameAs(connectionFactory);
        }

        @Test
        @DisplayName("uses default table name when not specified")
        void usesDefaultTableName() {
            ConnectionFactory connectionFactory = mock(ConnectionFactory.class);

            R2dbcOutboxConfig config = R2dbcOutboxConfig.builder()
                    .connectionFactory(connectionFactory)
                    .build();

            assertThat(config.tableName()).isEqualTo("outbox_events");
        }

        @Test
        @DisplayName("allows custom table name")
        void customTableName() {
            ConnectionFactory connectionFactory = mock(ConnectionFactory.class);

            R2dbcOutboxConfig config = R2dbcOutboxConfig.builder()
                    .connectionFactory(connectionFactory)
                    .tableName("custom_outbox")
                    .build();

            assertThat(config.tableName()).isEqualTo("custom_outbox");
        }

        @Test
        @DisplayName("throws exception when connection factory is null")
        void nullConnectionFactory() {
            assertThatThrownBy(() -> R2dbcOutboxConfig.builder().build())
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("connectionFactory");
        }
    }
}
