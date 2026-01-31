package xyz.charks.outbox.core;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OutboxStatusTest {

    @Test
    void pendingShouldNotBeTerminal() {
        Pending pending = Pending.create();

        assertThat(pending.isTerminal()).isFalse();
        assertThat(pending.canRetry()).isTrue();
    }

    @Test
    void publishedShouldBeTerminal() {
        Published published = Published.now();

        assertThat(published.isTerminal()).isTrue();
        assertThat(published.canRetry()).isFalse();
    }

    @Test
    void failedShouldNotBeTerminal() {
        Failed failed = Failed.firstAttempt("Connection refused");

        assertThat(failed.isTerminal()).isFalse();
        assertThat(failed.canRetry()).isTrue();
    }

    @Test
    void archivedShouldBeTerminal() {
        Archived archived = Archived.now();

        assertThat(archived.isTerminal()).isTrue();
        assertThat(archived.canRetry()).isFalse();
    }

    @Test
    void pendingShouldHaveCorrectName() {
        assertThat(Pending.create().name()).isEqualTo("PENDING");
    }

    @Test
    void publishedShouldHaveCorrectName() {
        assertThat(Published.now().name()).isEqualTo("PUBLISHED");
    }

    @Test
    void failedShouldHaveCorrectName() {
        assertThat(Failed.firstAttempt("error").name()).isEqualTo("FAILED");
    }

    @Test
    void archivedShouldHaveCorrectName() {
        assertThat(Archived.now().name()).isEqualTo("ARCHIVED");
    }

    @Test
    void pendingShouldCreateWithTimestamp() {
        Instant before = Instant.now();
        Pending pending = Pending.create();
        Instant after = Instant.now();

        assertThat(pending.enqueuedAt())
                .isAfterOrEqualTo(before)
                .isBeforeOrEqualTo(after);
    }

    @Test
    void pendingShouldCreateAtSpecificTime() {
        Instant timestamp = Instant.parse("2024-01-15T10:30:00Z");
        Pending pending = Pending.at(timestamp);

        assertThat(pending.enqueuedAt()).isEqualTo(timestamp);
    }

    @Test
    void pendingShouldRejectNullTimestamp() {
        assertThatThrownBy(() -> new Pending(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Enqueued at cannot be null");
    }

    @Test
    void publishedShouldCreateWithTimestamp() {
        Instant before = Instant.now();
        Published published = Published.now();
        Instant after = Instant.now();

        assertThat(published.publishedAt())
                .isAfterOrEqualTo(before)
                .isBeforeOrEqualTo(after);
    }

    @Test
    void publishedShouldCreateAtSpecificTime() {
        Instant timestamp = Instant.parse("2024-01-15T10:30:00Z");
        Published published = Published.at(timestamp);

        assertThat(published.publishedAt()).isEqualTo(timestamp);
    }

    @Test
    void publishedShouldRejectNullTimestamp() {
        assertThatThrownBy(() -> new Published(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Published at cannot be null");
    }

    @Test
    void failedShouldCaptureErrorAndAttemptCount() {
        Failed failed = Failed.of("Connection refused", 3);

        assertThat(failed.error()).isEqualTo("Connection refused");
        assertThat(failed.attemptCount()).isEqualTo(3);
        assertThat(failed.failedAt()).isNotNull();
    }

    @Test
    void failedFirstAttemptShouldSetCountToOne() {
        Failed failed = Failed.firstAttempt("Timeout");

        assertThat(failed.attemptCount()).isEqualTo(1);
    }

    @Test
    void failedShouldRejectNullError() {
        assertThatThrownBy(() -> new Failed(null, 1, Instant.now()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Error cannot be null");
    }

    @Test
    void failedShouldRejectNullTimestamp() {
        assertThatThrownBy(() -> new Failed("error", 1, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Failed at cannot be null");
    }

    @Test
    void failedShouldRejectNegativeAttemptCount() {
        assertThatThrownBy(() -> new Failed("error", -1, Instant.now()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Attempt count cannot be negative");
    }

    @Test
    void archivedShouldCreateWithTimestamp() {
        Instant before = Instant.now();
        Archived archived = Archived.now();
        Instant after = Instant.now();

        assertThat(archived.archivedAt())
                .isAfterOrEqualTo(before)
                .isBeforeOrEqualTo(after);
        assertThat(archived.reason()).isNull();
    }

    @Test
    void archivedShouldCreateWithReason() {
        Archived archived = Archived.withReason("Max retries exceeded");

        assertThat(archived.reason()).isEqualTo("Max retries exceeded");
    }

    @Test
    void archivedShouldCreateAtSpecificTime() {
        Instant timestamp = Instant.parse("2024-01-15T10:30:00Z");
        Archived archived = Archived.at(timestamp);

        assertThat(archived.archivedAt()).isEqualTo(timestamp);
        assertThat(archived.reason()).isNull();
    }

    @Test
    void archivedShouldRejectNullTimestamp() {
        assertThatThrownBy(() -> new Archived(null, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Archived at cannot be null");
    }

    @Test
    void shouldSupportPatternMatching() {
        OutboxStatus status = Pending.create();

        String result = switch (status) {
            case Pending p -> "pending since " + p.enqueuedAt();
            case Published p -> "published at " + p.publishedAt();
            case Failed f -> "failed: " + f.error();
            case Archived _ -> "archived";
        };

        assertThat(result).startsWith("pending since ");
    }
}
