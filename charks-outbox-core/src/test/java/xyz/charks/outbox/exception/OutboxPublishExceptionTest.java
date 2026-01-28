package xyz.charks.outbox.exception;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import xyz.charks.outbox.core.OutboxEventId;

import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("OutboxPublishException")
class OutboxPublishExceptionTest {

    @Nested
    @DisplayName("constructor with message")
    class MessageConstructorTest {

        @Test
        @DisplayName("stores message without event ID")
        void storesMessage() {
            OutboxPublishException exception = new OutboxPublishException("Broker unavailable");

            assertThat(exception.getMessage()).isEqualTo("Broker unavailable");
            assertThat(exception.getCause()).isNull();
            assertThat(exception.getEventId()).isNull();
        }
    }

    @Nested
    @DisplayName("constructor with message and cause")
    class MessageAndCauseConstructorTest {

        @Test
        @DisplayName("stores message and cause without event ID")
        void storesMessageAndCause() {
            TimeoutException cause = new TimeoutException("Timeout");

            OutboxPublishException exception = new OutboxPublishException("Broker unavailable", cause);

            assertThat(exception.getMessage()).isEqualTo("Broker unavailable");
            assertThat(exception.getCause()).isSameAs(cause);
            assertThat(exception.getEventId()).isNull();
        }

        @Test
        @DisplayName("accepts null cause")
        void acceptsNullCause() {
            OutboxPublishException exception = new OutboxPublishException("Broker unavailable", null);

            assertThat(exception.getMessage()).isEqualTo("Broker unavailable");
            assertThat(exception.getCause()).isNull();
            assertThat(exception.getEventId()).isNull();
        }
    }

    @Nested
    @DisplayName("constructor with event ID, message and cause")
    class EventIdMessageCauseConstructorTest {

        @Test
        @DisplayName("stores event ID, message and cause")
        void storesAllFields() {
            OutboxEventId eventId = new OutboxEventId(UUID.randomUUID());
            TimeoutException cause = new TimeoutException("Timeout");

            OutboxPublishException exception = new OutboxPublishException(eventId, "Publish failed", cause);

            assertThat(exception.getEventId()).isEqualTo(eventId);
            assertThat(exception.getMessage()).isEqualTo("Publish failed");
            assertThat(exception.getCause()).isSameAs(cause);
        }

        @Test
        @DisplayName("accepts null cause with event ID")
        void acceptsNullCause() {
            OutboxEventId eventId = new OutboxEventId(UUID.randomUUID());

            OutboxPublishException exception = new OutboxPublishException(eventId, "Publish failed", null);

            assertThat(exception.getEventId()).isEqualTo(eventId);
            assertThat(exception.getMessage()).isEqualTo("Publish failed");
            assertThat(exception.getCause()).isNull();
        }
    }

    @Nested
    @DisplayName("constructor with event ID and message")
    class EventIdMessageConstructorTest {

        @Test
        @DisplayName("stores event ID and message without cause")
        void storesEventIdAndMessage() {
            OutboxEventId eventId = new OutboxEventId(UUID.randomUUID());

            OutboxPublishException exception = new OutboxPublishException(eventId, "Publish failed");

            assertThat(exception.getEventId()).isEqualTo(eventId);
            assertThat(exception.getMessage()).isEqualTo("Publish failed");
            assertThat(exception.getCause()).isNull();
        }
    }

    @Nested
    @DisplayName("getEventId")
    class GetEventIdTest {

        @Test
        @DisplayName("returns event ID when set")
        void returnsEventId() {
            OutboxEventId eventId = new OutboxEventId(UUID.randomUUID());
            OutboxPublishException exception = new OutboxPublishException(eventId, "Error");

            assertThat(exception.getEventId()).isEqualTo(eventId);
        }

        @Test
        @DisplayName("returns null when not set")
        void returnsNullWhenNotSet() {
            OutboxPublishException exception = new OutboxPublishException("Error");

            assertThat(exception.getEventId()).isNull();
        }
    }

    @Nested
    @DisplayName("inheritance")
    class InheritanceTest {

        @Test
        @DisplayName("extends OutboxException")
        void extendsOutboxException() {
            OutboxPublishException exception = new OutboxPublishException("Test");

            assertThat(exception).isInstanceOf(OutboxException.class);
        }
    }
}
