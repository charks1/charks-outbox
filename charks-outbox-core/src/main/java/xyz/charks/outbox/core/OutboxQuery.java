package xyz.charks.outbox.core;

import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.Objects;
import java.util.Set;

/**
 * Criteria for querying outbox events.
 *
 * <p>Build queries using the fluent API:
 * <pre>{@code
 * OutboxQuery query = OutboxQuery.builder()
 *     .status(OutboxStatusFilter.PENDING)
 *     .aggregateType("Order")
 *     .createdBefore(Instant.now().minus(Duration.ofMinutes(5)))
 *     .limit(100)
 *     .lockMode(LockMode.FOR_UPDATE_SKIP_LOCKED)
 *     .build();
 * }</pre>
 *
 * @param statusFilter filter by event status (null for all statuses)
 * @param aggregateTypes filter by aggregate types (empty for all types)
 * @param topics filter by destination topics (empty for all topics)
 * @param createdAfter include events created after this time (inclusive)
 * @param createdBefore include events created before this time (exclusive)
 * @param maxRetryCount filter events with retry count at or below this value
 * @param limit maximum number of events to return
 * @param lockMode row locking mode for the query
 */
public record OutboxQuery(
        @Nullable OutboxStatusFilter statusFilter,
        Set<String> aggregateTypes,
        Set<String> topics,
        @Nullable Instant createdAfter,
        @Nullable Instant createdBefore,
        @Nullable Integer maxRetryCount,
        int limit,
        LockMode lockMode
) {

    /**
     * Creates an OutboxQuery with validation.
     *
     * @throws IllegalArgumentException if limit is not positive
     * @throws NullPointerException if aggregateTypes, topics, or lockMode is null
     */
    public OutboxQuery {
        Objects.requireNonNull(aggregateTypes, "Aggregate types cannot be null");
        Objects.requireNonNull(topics, "Topics cannot be null");
        Objects.requireNonNull(lockMode, "Lock mode cannot be null");

        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be positive");
        }
        if (maxRetryCount != null && maxRetryCount < 0) {
            throw new IllegalArgumentException("Max retry count cannot be negative");
        }

        aggregateTypes = Set.copyOf(aggregateTypes);
        topics = Set.copyOf(topics);
    }

    /**
     * Creates a query for pending events with default settings.
     *
     * @param limit maximum number of events to return
     * @return a query for pending events
     */
    public static OutboxQuery pending(int limit) {
        return builder()
                .status(OutboxStatusFilter.PENDING)
                .limit(limit)
                .build();
    }

    /**
     * Creates a query for pending events with locking.
     *
     * @param limit maximum number of events to return
     * @param lockMode the locking mode to use
     * @return a query for pending events with locking
     */
    public static OutboxQuery pendingWithLock(int limit, LockMode lockMode) {
        return builder()
                .status(OutboxStatusFilter.PENDING)
                .limit(limit)
                .lockMode(lockMode)
                .build();
    }

    /**
     * Creates a query for retryable events (pending or failed).
     *
     * @param limit maximum number of events to return
     * @param maxRetryCount maximum retry count to include
     * @return a query for retryable events
     */
    public static OutboxQuery retryable(int limit, int maxRetryCount) {
        return builder()
                .status(OutboxStatusFilter.RETRYABLE)
                .maxRetryCount(maxRetryCount)
                .limit(limit)
                .build();
    }

    /**
     * Creates a new builder for constructing OutboxQuery instances.
     *
     * @return a new OutboxQueryBuilder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for constructing OutboxQuery instances.
     */
    public static final class Builder {
        private @Nullable OutboxStatusFilter statusFilter;
        private Set<String> aggregateTypes = Set.of();
        private Set<String> topics = Set.of();
        private @Nullable Instant createdAfter;
        private @Nullable Instant createdBefore;
        private @Nullable Integer maxRetryCount;
        private int limit = 100;
        private LockMode lockMode = LockMode.NONE;

        private Builder() {}

        /**
         * Sets the status filter.
         *
         * @param status the status to filter by
         * @return this builder
         */
        public Builder status(@Nullable OutboxStatusFilter status) {
            this.statusFilter = status;
            return this;
        }

        /**
         * Sets the aggregate types to filter by.
         *
         * @param types the aggregate types
         * @return this builder
         */
        public Builder aggregateTypes(Set<String> types) {
            this.aggregateTypes = types;
            return this;
        }

        /**
         * Sets a single aggregate type to filter by.
         *
         * @param type the aggregate type
         * @return this builder
         */
        public Builder aggregateType(String type) {
            this.aggregateTypes = Set.of(type);
            return this;
        }

        /**
         * Sets the topics to filter by.
         *
         * @param topics the destination topics
         * @return this builder
         */
        public Builder topics(Set<String> topics) {
            this.topics = topics;
            return this;
        }

        /**
         * Sets a single topic to filter by.
         *
         * @param topic the destination topic
         * @return this builder
         */
        public Builder topic(String topic) {
            this.topics = Set.of(topic);
            return this;
        }

        /**
         * Sets the minimum creation time filter.
         *
         * @param createdAfter include events created at or after this time
         * @return this builder
         */
        public Builder createdAfter(@Nullable Instant createdAfter) {
            this.createdAfter = createdAfter;
            return this;
        }

        /**
         * Sets the maximum creation time filter.
         *
         * @param createdBefore include events created before this time
         * @return this builder
         */
        public Builder createdBefore(@Nullable Instant createdBefore) {
            this.createdBefore = createdBefore;
            return this;
        }

        /**
         * Sets the maximum retry count filter.
         *
         * @param maxRetryCount maximum retry count to include
         * @return this builder
         */
        public Builder maxRetryCount(@Nullable Integer maxRetryCount) {
            this.maxRetryCount = maxRetryCount;
            return this;
        }

        /**
         * Sets the maximum number of events to return.
         *
         * @param limit the limit
         * @return this builder
         */
        public Builder limit(int limit) {
            this.limit = limit;
            return this;
        }

        /**
         * Sets the row locking mode.
         *
         * @param lockMode the locking mode
         * @return this builder
         */
        public Builder lockMode(LockMode lockMode) {
            this.lockMode = lockMode;
            return this;
        }

        /**
         * Builds the OutboxQuery.
         *
         * @return a new OutboxQuery instance
         */
        public OutboxQuery build() {
            return new OutboxQuery(
                    statusFilter,
                    aggregateTypes,
                    topics,
                    createdAfter,
                    createdBefore,
                    maxRetryCount,
                    limit,
                    lockMode
            );
        }
    }
}
