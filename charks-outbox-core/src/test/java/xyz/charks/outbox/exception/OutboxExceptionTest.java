package xyz.charks.outbox.exception;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("OutboxException")
class OutboxExceptionTest {

    @Nested
    @DisplayName("constructor with message")
    class MessageConstructorTest {

        @Test
        @DisplayName("stores message")
        void storesMessage() {
            OutboxException exception = new OutboxException("Test error");

            assertThat(exception.getMessage()).isEqualTo("Test error");
            assertThat(exception.getCause()).isNull();
        }
    }

    @Nested
    @DisplayName("constructor with message and cause")
    class MessageAndCauseConstructorTest {

        @Test
        @DisplayName("stores message and cause")
        void storesMessageAndCause() {
            RuntimeException cause = new RuntimeException("Root cause");

            OutboxException exception = new OutboxException("Test error", cause);

            assertThat(exception.getMessage()).isEqualTo("Test error");
            assertThat(exception.getCause()).isSameAs(cause);
        }

        @Test
        @DisplayName("accepts null cause")
        void acceptsNullCause() {
            OutboxException exception = new OutboxException("Test error", null);

            assertThat(exception.getMessage()).isEqualTo("Test error");
            assertThat(exception.getCause()).isNull();
        }
    }

    @Nested
    @DisplayName("constructor with cause")
    class CauseConstructorTest {

        @Test
        @DisplayName("stores cause and uses its message")
        void storesCause() {
            RuntimeException cause = new RuntimeException("Root cause");

            OutboxException exception = new OutboxException(cause);

            assertThat(exception.getCause()).isSameAs(cause);
            assertThat(exception.getMessage()).contains("Root cause");
        }
    }

    @Nested
    @DisplayName("inheritance")
    class InheritanceTest {

        @Test
        @DisplayName("is a RuntimeException")
        void isRuntimeException() {
            OutboxException exception = new OutboxException("Test");

            assertThat(exception).isInstanceOf(RuntimeException.class);
        }
    }
}
