package xyz.charks.outbox.mongodb;

import com.mongodb.client.MongoClient;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

@DisplayName("MongoOutboxConfig")
class MongoOutboxConfigTest {

    @Nested
    @DisplayName("builder")
    class BuilderTest {

        @Test
        @DisplayName("creates config with required client")
        void createsWithClient() {
            MongoClient client = mock(MongoClient.class);

            MongoOutboxConfig config = MongoOutboxConfig.builder()
                    .client(client)
                    .build();

            assertThat(config.client()).isSameAs(client);
        }

        @Test
        @DisplayName("uses default database name when not specified")
        void usesDefaultDatabase() {
            MongoClient client = mock(MongoClient.class);

            MongoOutboxConfig config = MongoOutboxConfig.builder()
                    .client(client)
                    .build();

            assertThat(config.database()).isEqualTo("outbox");
        }

        @Test
        @DisplayName("uses default collection name when not specified")
        void usesDefaultCollection() {
            MongoClient client = mock(MongoClient.class);

            MongoOutboxConfig config = MongoOutboxConfig.builder()
                    .client(client)
                    .build();

            assertThat(config.collection()).isEqualTo("outbox_events");
        }

        @Test
        @DisplayName("allows custom database name")
        void customDatabase() {
            MongoClient client = mock(MongoClient.class);

            MongoOutboxConfig config = MongoOutboxConfig.builder()
                    .client(client)
                    .database("custom_db")
                    .build();

            assertThat(config.database()).isEqualTo("custom_db");
        }

        @Test
        @DisplayName("allows custom collection name")
        void customCollection() {
            MongoClient client = mock(MongoClient.class);

            MongoOutboxConfig config = MongoOutboxConfig.builder()
                    .client(client)
                    .collection("custom_collection")
                    .build();

            assertThat(config.collection()).isEqualTo("custom_collection");
        }

        @Test
        @DisplayName("throws exception when client is null")
        void nullClient() {
            assertThatThrownBy(() -> MongoOutboxConfig.builder().build())
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("client");
        }
    }
}
