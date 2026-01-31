package xyz.charks.outbox.broker;

import xyz.charks.outbox.core.OutboxEventId;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PublishResultTest {

    @Test
    void shouldCreateSuccessResult() {
        OutboxEventId eventId = OutboxEventId.generate();
        Instant before = Instant.now();

        PublishResult result = PublishResult.success(eventId);

        assertThat(result.success()).isTrue();
        assertThat(result.isFailure()).isFalse();
        assertThat(result.eventId()).isEqualTo(eventId);
        assertThat(result.publishedAt()).isAfterOrEqualTo(before);
        assertThat(result.error()).isNull();
    }

    @Test
    void shouldCreateSuccessResultWithMetadata() {
        OutboxEventId eventId = OutboxEventId.generate();

        PublishResult result = PublishResult.success(eventId, "msg-123", 2, 456L);

        assertThat(result.success()).isTrue();
        assertThat(result.getBrokerMessageId()).hasValue("msg-123");
        assertThat(result.getPartition()).hasValue(2);
        assertThat(result.getOffset()).hasValue(456L);
    }

    @Test
    void shouldCreateFailureResult() {
        OutboxEventId eventId = OutboxEventId.generate();

        PublishResult result = PublishResult.failure(eventId, "Connection refused");

        assertThat(result.success()).isFalse();
        assertThat(result.isFailure()).isTrue();
        assertThat(result.eventId()).isEqualTo(eventId);
        assertThat(result.getError()).hasValue("Connection refused");
        assertThat(result.publishedAt()).isNull();
    }

    @Test
    void shouldCreateFailureResultFromException() {
        OutboxEventId eventId = OutboxEventId.generate();
        RuntimeException cause = new RuntimeException("Timeout");

        PublishResult result = PublishResult.failure(eventId, cause);

        assertThat(result.success()).isFalse();
        assertThat(result.getError()).hasValue("Timeout");
    }

    @Test
    void shouldHandleExceptionWithNullMessage() {
        OutboxEventId eventId = OutboxEventId.generate();
        RuntimeException cause = new RuntimeException((String) null);

        PublishResult result = PublishResult.failure(eventId, cause);

        assertThat(result.getError()).hasValue("java.lang.RuntimeException");
    }

    @Test
    @SuppressWarnings("DataFlowIssue")
    void shouldRejectNullEventId() {
        assertThatThrownBy(() -> PublishResult.success(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Event ID cannot be null");
    }

    @Test
    @SuppressWarnings("DataFlowIssue")
    void shouldRejectNullErrorMessage() {
        OutboxEventId eventId = OutboxEventId.generate();

        assertThatThrownBy(() -> PublishResult.failure(eventId, (String) null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldReturnEmptyOptionalForMissingMetadata() {
        OutboxEventId eventId = OutboxEventId.generate();
        PublishResult result = PublishResult.success(eventId);

        assertThat(result.getBrokerMessageId()).isEmpty();
        assertThat(result.getPartition()).isEmpty();
        assertThat(result.getOffset()).isEmpty();
    }

    @Test
    void shouldReturnEmptyOptionalForErrorOnSuccess() {
        OutboxEventId eventId = OutboxEventId.generate();
        PublishResult result = PublishResult.success(eventId);

        assertThat(result.getError()).isEmpty();
    }

    @Test
    void shouldHaveValueBasedEquality() {
        OutboxEventId eventId = OutboxEventId.generate();
        Instant timestamp = Instant.now();

        PublishResult result1 = new PublishResult(eventId, true, timestamp, "msg-1", 0, 100L, null);
        PublishResult result2 = new PublishResult(eventId, true, timestamp, "msg-1", 0, 100L, null);

        assertThat(result1).isEqualTo(result2).hasSameHashCodeAs(result2);
    }
}
