package xyz.charks.outbox.exception;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("OutboxPersistenceException")
class OutboxPersistenceExceptionTest {

    @Nested
    @DisplayName("constructor with message")
    class MessageConstructorTest {

        @Test
        @DisplayName("stores message")
        void storesMessage() {
            OutboxPersistenceException exception = new OutboxPersistenceException("Database error");

            assertThat(exception.getMessage()).isEqualTo("Database error");
            assertThat(exception.getCause()).isNull();
        }
    }

    @Nested
    @DisplayName("constructor with message and cause")
    class MessageAndCauseConstructorTest {

        @Test
        @DisplayName("stores message and cause")
        void storesMessageAndCause() {
            SQLException cause = new SQLException("Connection failed");

            OutboxPersistenceException exception = new OutboxPersistenceException("Database error", cause);

            assertThat(exception.getMessage()).isEqualTo("Database error");
            assertThat(exception.getCause()).isSameAs(cause);
        }

        @Test
        @DisplayName("accepts null cause")
        void acceptsNullCause() {
            OutboxPersistenceException exception = new OutboxPersistenceException("Database error", null);

            assertThat(exception.getMessage()).isEqualTo("Database error");
            assertThat(exception.getCause()).isNull();
        }
    }

    @Nested
    @DisplayName("constructor with cause")
    class CauseConstructorTest {

        @Test
        @DisplayName("stores cause")
        void storesCause() {
            SQLException cause = new SQLException("Connection failed");

            OutboxPersistenceException exception = new OutboxPersistenceException(cause);

            assertThat(exception.getCause()).isSameAs(cause);
        }
    }

    @Nested
    @DisplayName("inheritance")
    class InheritanceTest {

        @Test
        @DisplayName("extends OutboxException")
        void extendsOutboxException() {
            OutboxPersistenceException exception = new OutboxPersistenceException("Test");

            assertThat(exception).isInstanceOf(OutboxException.class);
        }
    }
}
