package xyz.charks.outbox.core;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OutboxEventIdTest {

    @Test
    void shouldCreateWithUuid() {
        UUID uuid = UUID.randomUUID();
        OutboxEventId id = new OutboxEventId(uuid);

        assertThat(id.value()).isEqualTo(uuid);
    }

    @Test
    void shouldRejectNullUuid() {
        assertThatThrownBy(() -> new OutboxEventId(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Event ID cannot be null");
    }

    @Test
    void shouldGenerateRandomId() {
        OutboxEventId id1 = OutboxEventId.generate();
        OutboxEventId id2 = OutboxEventId.generate();

        assertThat(id1).isNotEqualTo(id2);
        assertThat(id1.value()).isNotNull();
    }

    @Test
    void shouldCreateFromString() {
        String uuidString = "550e8400-e29b-41d4-a716-446655440000";
        OutboxEventId id = OutboxEventId.fromString(uuidString);

        assertThat(id.value()).isEqualTo(UUID.fromString(uuidString));
    }

    @Test
    void shouldRejectInvalidUuidString() {
        assertThatThrownBy(() -> OutboxEventId.fromString("not-a-uuid"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectNullString() {
        assertThatThrownBy(() -> OutboxEventId.fromString(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("ID string cannot be null");
    }

    @Test
    void shouldHaveValueBasedEquality() {
        UUID uuid = UUID.randomUUID();
        OutboxEventId id1 = new OutboxEventId(uuid);
        OutboxEventId id2 = new OutboxEventId(uuid);

        assertThat(id1).isEqualTo(id2);
        assertThat(id1.hashCode()).isEqualTo(id2.hashCode());
    }

    @Test
    void shouldReturnUuidStringRepresentation() {
        UUID uuid = UUID.randomUUID();
        OutboxEventId id = new OutboxEventId(uuid);

        assertThat(id.toString()).isEqualTo(uuid.toString());
    }
}
