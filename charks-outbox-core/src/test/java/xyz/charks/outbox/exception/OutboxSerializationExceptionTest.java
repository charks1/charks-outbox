package xyz.charks.outbox.exception;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("OutboxSerializationException")
class OutboxSerializationExceptionTest {

    @Nested
    @DisplayName("constructor with message")
    class MessageConstructorTest {

        @Test
        @DisplayName("stores message")
        void storesMessage() {
            OutboxSerializationException exception = new OutboxSerializationException("Invalid JSON");

            assertThat(exception.getMessage()).isEqualTo("Invalid JSON");
            assertThat(exception.getCause()).isNull();
        }
    }

    @Nested
    @DisplayName("constructor with message and cause")
    class MessageAndCauseConstructorTest {

        @Test
        @DisplayName("stores message and cause")
        void storesMessageAndCause() {
            RuntimeException cause = new RuntimeException("Parse error");

            OutboxSerializationException exception = new OutboxSerializationException("Invalid JSON", cause);

            assertThat(exception.getMessage()).isEqualTo("Invalid JSON");
            assertThat(exception.getCause()).isSameAs(cause);
        }

        @Test
        @DisplayName("accepts null cause")
        void acceptsNullCause() {
            OutboxSerializationException exception = new OutboxSerializationException("Invalid JSON", null);

            assertThat(exception.getMessage()).isEqualTo("Invalid JSON");
            assertThat(exception.getCause()).isNull();
        }
    }

    @Nested
    @DisplayName("constructor with cause")
    class CauseConstructorTest {

        @Test
        @DisplayName("stores cause")
        void storesCause() {
            RuntimeException cause = new RuntimeException("Parse error");

            OutboxSerializationException exception = new OutboxSerializationException(cause);

            assertThat(exception.getCause()).isSameAs(cause);
        }
    }

    @Nested
    @DisplayName("inheritance")
    class InheritanceTest {

        @Test
        @DisplayName("extends OutboxException")
        void extendsOutboxException() {
            OutboxSerializationException exception = new OutboxSerializationException("Test");

            assertThat(exception).isInstanceOf(OutboxException.class);
        }
    }
}
