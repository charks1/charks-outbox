package xyz.charks.outbox.core;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AggregateIdTest {

    @Test
    void shouldCreateWithStringValue() {
        AggregateId id = new AggregateId("order-123");

        assertThat(id.value()).isEqualTo("order-123");
    }

    @Test
    @SuppressWarnings("DataFlowIssue") // intentionally passing null to test rejection
    void shouldRejectNullValue() {
        assertThatThrownBy(() -> new AggregateId(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Aggregate ID cannot be null");
    }

    @Test
    void shouldAllowEmptyValue() {
        AggregateId id = new AggregateId("");

        assertThat(id.value()).isEmpty();
        assertThat(id.isEmpty()).isTrue();
    }

    @Test
    void shouldCreateFromObject() {
        UUID uuid = UUID.randomUUID();
        AggregateId id = AggregateId.of(uuid);

        assertThat(id.value()).isEqualTo(uuid.toString());
    }

    @Test
    void shouldCreateFromInteger() {
        AggregateId id = AggregateId.of(42);

        assertThat(id.value()).isEqualTo("42");
    }

    @Test
    @SuppressWarnings("DataFlowIssue") // intentionally passing null to test rejection
    void shouldRejectNullObject() {
        assertThatThrownBy(() -> AggregateId.of(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("ID cannot be null");
    }

    @Test
    void shouldCreateEmptyId() {
        AggregateId id = AggregateId.empty();

        assertThat(id.value()).isEmpty();
        assertThat(id.isEmpty()).isTrue();
    }

    @Test
    void shouldDetectNonEmptyId() {
        AggregateId id = new AggregateId("some-id");

        assertThat(id.isEmpty()).isFalse();
    }

    @Test
    void shouldHaveValueBasedEquality() {
        AggregateId id1 = new AggregateId("order-123");
        AggregateId id2 = new AggregateId("order-123");

        assertThat(id1).isEqualTo(id2).hasSameHashCodeAs(id2);
    }

    @Test
    void shouldReturnValueAsString() {
        AggregateId id = new AggregateId("order-123");

        assertThat(id).hasToString("order-123");
    }
}
