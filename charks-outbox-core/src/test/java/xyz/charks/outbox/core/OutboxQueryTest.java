package xyz.charks.outbox.core;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OutboxQueryTest {

    @Test
    void shouldBuildWithDefaults() {
        OutboxQuery query = OutboxQuery.builder().build();

        assertThat(query.statusFilter()).isNull();
        assertThat(query.aggregateTypes()).isEmpty();
        assertThat(query.topics()).isEmpty();
        assertThat(query.createdAfter()).isNull();
        assertThat(query.createdBefore()).isNull();
        assertThat(query.maxRetryCount()).isNull();
        assertThat(query.limit()).isEqualTo(100);
        assertThat(query.lockMode()).isEqualTo(LockMode.NONE);
    }

    @Test
    void shouldBuildFullyConfiguredQuery() {
        Instant after = Instant.parse("2024-01-01T00:00:00Z");
        Instant before = Instant.parse("2024-01-31T23:59:59Z");

        OutboxQuery query = OutboxQuery.builder()
                .status(OutboxStatusFilter.PENDING)
                .aggregateTypes(Set.of("Order", "Customer"))
                .topics(Set.of("orders", "customers"))
                .createdAfter(after)
                .createdBefore(before)
                .maxRetryCount(3)
                .limit(50)
                .lockMode(LockMode.FOR_UPDATE_SKIP_LOCKED)
                .build();

        assertThat(query.statusFilter()).isEqualTo(OutboxStatusFilter.PENDING);
        assertThat(query.aggregateTypes()).containsExactlyInAnyOrder("Order", "Customer");
        assertThat(query.topics()).containsExactlyInAnyOrder("orders", "customers");
        assertThat(query.createdAfter()).isEqualTo(after);
        assertThat(query.createdBefore()).isEqualTo(before);
        assertThat(query.maxRetryCount()).isEqualTo(3);
        assertThat(query.limit()).isEqualTo(50);
        assertThat(query.lockMode()).isEqualTo(LockMode.FOR_UPDATE_SKIP_LOCKED);
    }

    @Test
    void shouldBuildWithSingleAggregateType() {
        OutboxQuery query = OutboxQuery.builder()
                .aggregateType("Order")
                .build();

        assertThat(query.aggregateTypes()).containsExactly("Order");
    }

    @Test
    void shouldBuildWithSingleTopic() {
        OutboxQuery query = OutboxQuery.builder()
                .topic("orders")
                .build();

        assertThat(query.topics()).containsExactly("orders");
    }

    @Test
    void shouldCreatePendingQuery() {
        OutboxQuery query = OutboxQuery.pending(50);

        assertThat(query.statusFilter()).isEqualTo(OutboxStatusFilter.PENDING);
        assertThat(query.limit()).isEqualTo(50);
        assertThat(query.lockMode()).isEqualTo(LockMode.NONE);
    }

    @Test
    void shouldCreatePendingQueryWithLock() {
        OutboxQuery query = OutboxQuery.pendingWithLock(100, LockMode.FOR_UPDATE_SKIP_LOCKED);

        assertThat(query.statusFilter()).isEqualTo(OutboxStatusFilter.PENDING);
        assertThat(query.limit()).isEqualTo(100);
        assertThat(query.lockMode()).isEqualTo(LockMode.FOR_UPDATE_SKIP_LOCKED);
    }

    @Test
    void shouldCreateRetryableQuery() {
        OutboxQuery query = OutboxQuery.retryable(25, 5);

        assertThat(query.statusFilter()).isEqualTo(OutboxStatusFilter.RETRYABLE);
        assertThat(query.maxRetryCount()).isEqualTo(5);
        assertThat(query.limit()).isEqualTo(25);
    }

    @Test
    void shouldRejectNonPositiveLimit() {
        assertThatThrownBy(() -> OutboxQuery.builder().limit(0).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Limit must be positive");

        assertThatThrownBy(() -> OutboxQuery.builder().limit(-1).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Limit must be positive");
    }

    @Test
    void shouldRejectNegativeMaxRetryCount() {
        assertThatThrownBy(() -> OutboxQuery.builder().maxRetryCount(-1).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Max retry count cannot be negative");
    }

    @Test
    void shouldReturnImmutableAggregateTypes() {
        OutboxQuery query = OutboxQuery.builder()
                .aggregateTypes(Set.of("Order"))
                .build();

        assertThatThrownBy(() -> query.aggregateTypes().add("Customer"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void shouldReturnImmutableTopics() {
        OutboxQuery query = OutboxQuery.builder()
                .topics(Set.of("orders"))
                .build();

        assertThatThrownBy(() -> query.topics().add("customers"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void shouldHaveValueBasedEquality() {
        OutboxQuery query1 = OutboxQuery.builder()
                .status(OutboxStatusFilter.PENDING)
                .limit(50)
                .build();

        OutboxQuery query2 = OutboxQuery.builder()
                .status(OutboxStatusFilter.PENDING)
                .limit(50)
                .build();

        assertThat(query1).isEqualTo(query2);
        assertThat(query1.hashCode()).isEqualTo(query2.hashCode());
    }
}
