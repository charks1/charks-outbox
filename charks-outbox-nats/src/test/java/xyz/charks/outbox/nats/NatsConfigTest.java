package xyz.charks.outbox.nats;

import io.nats.client.Connection;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

@DisplayName("NatsConfig")
class NatsConfigTest {

    @Nested
    @DisplayName("builder")
    class BuilderTest {

        @Test
        @DisplayName("creates config with required connection")
        void createsWithConnection() {
            Connection connection = mock(Connection.class);

            NatsConfig config = NatsConfig.builder()
                    .connection(connection)
                    .build();

            assertThat(config.connection()).isSameAs(connection);
        }

        @Test
        @DisplayName("uses default subject prefix when not specified")
        void usesDefaultSubjectPrefix() {
            Connection connection = mock(Connection.class);

            NatsConfig config = NatsConfig.builder()
                    .connection(connection)
                    .build();

            assertThat(config.subjectPrefix()).isEqualTo("outbox.");
        }

        @Test
        @DisplayName("allows custom subject prefix")
        void customSubjectPrefix() {
            Connection connection = mock(Connection.class);

            NatsConfig config = NatsConfig.builder()
                    .connection(connection)
                    .subjectPrefix("events.")
                    .build();

            assertThat(config.subjectPrefix()).isEqualTo("events.");
        }

        @Test
        @DisplayName("throws exception when connection is null")
        void nullConnection() {
            assertThatThrownBy(() -> NatsConfig.builder().build())
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("connection");
        }
    }
}
