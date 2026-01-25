package xyz.charks.outbox.core;

/**
 * Filter options for querying outbox events by status.
 *
 * <p>Used with {@link OutboxQuery} and {@link OutboxRepository#count(OutboxStatusFilter)}
 * to filter events based on their lifecycle state.
 *
 * @see OutboxStatus
 * @see OutboxQuery
 */
public enum OutboxStatusFilter {

    /** Only pending events waiting to be published. */
    PENDING,

    /** Only successfully published events. */
    PUBLISHED,

    /** Only failed events that encountered errors. */
    FAILED,

    /** Only archived events removed from active processing. */
    ARCHIVED,

    /** Pending or failed events eligible for retry. */
    RETRYABLE
}
