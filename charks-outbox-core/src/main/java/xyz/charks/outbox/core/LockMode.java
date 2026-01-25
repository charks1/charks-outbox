package xyz.charks.outbox.core;

/**
 * Defines the row locking strategy for retrieving pending events.
 *
 * <p>The locking mode determines how concurrent relay instances behave
 * when processing the outbox table:
 *
 * <ul>
 *   <li>{@link #FOR_UPDATE} - Blocks other transactions from reading selected rows.
 *       Suitable for single-instance deployments.</li>
 *   <li>{@link #FOR_UPDATE_SKIP_LOCKED} - Skips rows already locked by other transactions.
 *       Recommended for clustered deployments to enable parallel processing.</li>
 *   <li>{@link #NONE} - No locking. Use only when duplicate processing is acceptable
 *       or external coordination ensures single-consumer access.</li>
 * </ul>
 *
 * <p>Example SQL for PostgreSQL:
 * <pre>{@code
 * -- FOR_UPDATE
 * SELECT * FROM outbox_events WHERE status = 'PENDING'
 * ORDER BY created_at FOR UPDATE
 *
 * -- FOR_UPDATE_SKIP_LOCKED
 * SELECT * FROM outbox_events WHERE status = 'PENDING'
 * ORDER BY created_at FOR UPDATE SKIP LOCKED
 * }</pre>
 */
public enum LockMode {

    /**
     * Standard pessimistic lock that blocks concurrent readers.
     *
     * <p>Use when only a single relay instance processes the outbox.
     */
    FOR_UPDATE,

    /**
     * Lock that skips already-locked rows without blocking.
     *
     * <p>Use in clustered deployments where multiple relay instances
     * process the same outbox table concurrently.
     */
    FOR_UPDATE_SKIP_LOCKED,

    /**
     * No database-level locking.
     *
     * <p>Use when:
     * <ul>
     *   <li>The broker guarantees idempotent delivery</li>
     *   <li>Application-level coordination prevents concurrent access</li>
     *   <li>Duplicate processing is acceptable</li>
     * </ul>
     */
    NONE
}
