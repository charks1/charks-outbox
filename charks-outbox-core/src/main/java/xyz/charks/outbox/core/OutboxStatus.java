package xyz.charks.outbox.core;

/**
 * Sealed interface representing the lifecycle status of an outbox event.
 *
 * <p>An outbox event progresses through these states:
 * <pre>
 *                    ┌──────────┐
 *                    │ Pending  │
 *                    └────┬─────┘
 *                         │
 *            ┌────────────┼────────────┐
 *            ▼            ▼            │
 *     ┌──────────┐  ┌──────────┐       │ (retry)
 *     │Published │  │  Failed  │───────┘
 *     └────┬─────┘  └────┬─────┘
 *          │             │
 *          │             ▼
 *          │       ┌──────────┐
 *          └──────►│ Archived │
 *                  └──────────┘
 * </pre>
 *
 * <p>Pattern matching can be used to handle each status:
 * <pre>{@code
 * return switch (event.status()) {
 *     case Pending p when p.retryCount() < maxRetries -> scheduleRetry();
 *     case Pending p -> moveToDeadLetter();
 *     case Published _ -> logSuccess();
 *     case Failed f -> notifyAlert(f.error());
 *     case Archived _ -> skip();
 * };
 * }</pre>
 *
 * @see Pending
 * @see Published
 * @see Failed
 * @see Archived
 */
public sealed interface OutboxStatus permits Pending, Published, Failed, Archived {

    /**
     * Returns the string representation of this status for persistence.
     *
     * @return the status name (PENDING, PUBLISHED, FAILED, ARCHIVED)
     */
    String name();

    /**
     * Checks if the event is in a terminal state (Published or Archived).
     *
     * @return true if the event will not be processed further
     */
    default boolean isTerminal() {
        return this instanceof Published || this instanceof Archived;
    }

    /**
     * Checks if the event can be retried.
     *
     * @return true if the event is eligible for retry
     */
    default boolean canRetry() {
        return this instanceof Pending || this instanceof Failed;
    }
}
