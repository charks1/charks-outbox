package xyz.charks.outbox.sqs;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.SqsClient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

@DisplayName("SqsConfig")
class SqsConfigTest {

    @Nested
    @DisplayName("builder")
    class BuilderTest {

        @Test
        @DisplayName("creates config with required fields")
        void createsWithRequiredFields() {
            SqsClient client = mock(SqsClient.class);

            SqsConfig config = SqsConfig.builder()
                    .client(client)
                    .queueUrl("https://sqs.us-east-1.amazonaws.com/123456789/test-queue")
                    .build();

            assertThat(config.client()).isSameAs(client);
            assertThat(config.queueUrl()).isEqualTo("https://sqs.us-east-1.amazonaws.com/123456789/test-queue");
        }

        @Test
        @DisplayName("uses default delay of zero")
        void usesDefaultDelay() {
            SqsClient client = mock(SqsClient.class);

            SqsConfig config = SqsConfig.builder()
                    .client(client)
                    .queueUrl("https://sqs.us-east-1.amazonaws.com/123456789/test-queue")
                    .build();

            assertThat(config.delaySeconds()).isZero();
        }

        @Test
        @DisplayName("allows custom delay")
        void customDelay() {
            SqsClient client = mock(SqsClient.class);

            SqsConfig config = SqsConfig.builder()
                    .client(client)
                    .queueUrl("https://sqs.us-east-1.amazonaws.com/123456789/test-queue")
                    .delaySeconds(30)
                    .build();

            assertThat(config.delaySeconds()).isEqualTo(30);
        }

        @Test
        @DisplayName("throws exception when client is null")
        void nullClient() {
            assertThatThrownBy(() -> SqsConfig.builder()
                    .queueUrl("https://sqs.us-east-1.amazonaws.com/123456789/test-queue")
                    .build())
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("client");
        }

        @Test
        @DisplayName("throws exception when queue URL is null")
        void nullQueueUrl() {
            SqsClient client = mock(SqsClient.class);

            assertThatThrownBy(() -> SqsConfig.builder()
                    .client(client)
                    .build())
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("queueUrl");
        }
    }
}
