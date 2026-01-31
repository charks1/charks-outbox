package xyz.charks.outbox.core;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EventTypeTest {

    @Test
    void shouldCreateWithValue() {
        EventType type = new EventType("OrderCreated");

        assertThat(type.value()).isEqualTo("OrderCreated");
    }

    @Test
    @SuppressWarnings("DataFlowIssue") // intentionally passing null to test rejection
    void shouldRejectNullValue() {
        assertThatThrownBy(() -> new EventType(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Event type cannot be null");
    }

    @Test
    void shouldRejectBlankValue() {
        assertThatThrownBy(() -> new EventType(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Event type cannot be blank");

        assertThatThrownBy(() -> new EventType("   "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Event type cannot be blank");
    }

    @Test
    void shouldCreateFromClass() {
        EventType type = EventType.fromClass(String.class);

        assertThat(type.value()).isEqualTo("java.lang.String");
    }

    @Test
    @SuppressWarnings("DataFlowIssue") // intentionally passing null to test rejection
    void shouldRejectNullClass() {
        assertThatThrownBy(() -> EventType.fromClass(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Event class cannot be null");
    }

    @Test
    void shouldCreateFromSimpleName() {
        EventType type = EventType.fromSimpleName(String.class);

        assertThat(type.value()).isEqualTo("String");
    }

    @Test
    @SuppressWarnings("DataFlowIssue") // intentionally passing null to test rejection
    void shouldRejectNullClassForSimpleName() {
        assertThatThrownBy(() -> EventType.fromSimpleName(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Event class cannot be null");
    }

    @Test
    void shouldCreateUsingOfFactory() {
        EventType type = EventType.of("order.created.v1");

        assertThat(type.value()).isEqualTo("order.created.v1");
    }

    @Test
    void shouldHaveValueBasedEquality() {
        EventType type1 = new EventType("OrderCreated");
        EventType type2 = new EventType("OrderCreated");

        assertThat(type1).isEqualTo(type2).hasSameHashCodeAs(type2);
    }

    @Test
    void shouldReturnValueAsString() {
        EventType type = new EventType("OrderCreated");

        assertThat(type).hasToString("OrderCreated");
    }
}
