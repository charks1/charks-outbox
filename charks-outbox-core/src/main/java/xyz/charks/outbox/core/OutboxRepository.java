package xyz.charks.outbox.core;

import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Optional;

/**
 * Repository interface for outbox event persistence operations.
 *
 * <p>Implementations must ensure that all operations participate in the current
 * transaction context. The outbox pattern relies on events being written to the
 * database in the same transaction as the business operation.
 *
 * <p>Typical implementations:
 * <ul>
 *   <li>JDBC-based for synchronous, connection-pool environments</li>
 *   <li>R2DBC-based for reactive applications</li>
 *   <li>MongoDB-based for document store deployments</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * // Within a transaction
 * orderRepository.save(order);
 * outboxRepository.save(OutboxEvent.builder()
 *     .aggregateType("Order")
 *     .aggregateId(order.getId().toString())
 *     .eventType("OrderCreated")
 *     .topic("orders")
 *     .payload(serializer.serialize(orderCreatedEvent))
 *     .build());
 * }</pre>
 *
 * @see OutboxEvent
 * @see OutboxQuery
 */
public interface OutboxRepository {

    /**
     * Persists a new outbox event.
     *
     * <p>The event must be saved within the current transaction to ensure
     * atomicity with the business operation.
     *
     * @param event the event to save
     * @return the saved event (may include generated values)
     * @throws xyz.charks.outbox.exception.OutboxPersistenceException if persistence fails
     * @throws NullPointerException if event is null
     */
    OutboxEvent save(OutboxEvent event);

    /**
     * Persists multiple outbox events in a single operation.
     *
     * <p>All events must be saved within the current transaction.
     *
     * @param events the events to save
     * @return the saved events
     * @throws xyz.charks.outbox.exception.OutboxPersistenceException if persistence fails
     * @throws NullPointerException if events is null
     */
    List<OutboxEvent> saveAll(List<OutboxEvent> events);

    /**
     * Finds an event by its unique identifier.
     *
     * @param id the event identifier
     * @return the event if found, empty otherwise
     * @throws NullPointerException if id is null
     */
    Optional<OutboxEvent> findById(OutboxEventId id);

    /**
     * Finds events matching the specified query criteria.
     *
     * <p>Events are returned in creation order (oldest first) to ensure
     * FIFO processing. The query's lock mode determines whether rows
     * are locked for update.
     *
     * @param query the search criteria
     * @return matching events, up to the query's limit
     * @throws xyz.charks.outbox.exception.OutboxPersistenceException if the query fails
     * @throws NullPointerException if query is null
     */
    List<OutboxEvent> find(OutboxQuery query);

    /**
     * Updates an existing outbox event.
     *
     * <p>Typically used to update the event status after processing.
     *
     * @param event the event with updated values
     * @return the updated event
     * @throws xyz.charks.outbox.exception.OutboxPersistenceException if update fails or event not found
     * @throws NullPointerException if event is null
     */
    OutboxEvent update(OutboxEvent event);

    /**
     * Updates the status of multiple events.
     *
     * <p>Optimized batch operation for marking events as published or failed.
     *
     * @param ids the event identifiers
     * @param status the new status to set
     * @return the number of events updated
     * @throws xyz.charks.outbox.exception.OutboxPersistenceException if update fails
     * @throws NullPointerException if ids or status is null
     */
    int updateStatus(List<OutboxEventId> ids, OutboxStatus status);

    /**
     * Deletes an event by its identifier.
     *
     * <p>Use for cleanup of successfully processed events.
     *
     * @param id the event identifier
     * @return true if the event was deleted, false if not found
     * @throws xyz.charks.outbox.exception.OutboxPersistenceException if deletion fails
     * @throws NullPointerException if id is null
     */
    boolean deleteById(OutboxEventId id);

    /**
     * Deletes events matching the specified query criteria.
     *
     * <p>Useful for bulk cleanup of old published or archived events.
     *
     * @param query the criteria for events to delete
     * @return the number of events deleted
     * @throws xyz.charks.outbox.exception.OutboxPersistenceException if deletion fails
     * @throws NullPointerException if query is null
     */
    int delete(OutboxQuery query);

    /**
     * Counts events matching the specified status.
     *
     * <p>Useful for monitoring and metrics.
     *
     * @param statusFilter the status to count (null for all)
     * @return the count of matching events
     */
    long count(@Nullable OutboxStatusFilter statusFilter);
}
