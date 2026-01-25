package xyz.charks.outbox.r2dbc;

import org.reactivestreams.Publisher;
import xyz.charks.outbox.core.OutboxEvent;
import xyz.charks.outbox.core.OutboxEventId;
import xyz.charks.outbox.core.OutboxQuery;

import java.util.Collection;

/**
 * Reactive interface for outbox repository operations.
 *
 * <p>This interface provides non-blocking, reactive operations for managing
 * outbox events using Project Reactor's Publisher type.
 *
 * @see R2dbcOutboxRepository
 */
public interface ReactiveOutboxRepository {

    /**
     * Saves an event reactively.
     *
     * @param event the event to save
     * @return a publisher that completes when the event is saved
     */
    Publisher<Void> save(OutboxEvent event);

    /**
     * Saves multiple events reactively.
     *
     * @param events the events to save
     * @return a publisher that completes when all events are saved
     */
    Publisher<Void> saveAll(Collection<OutboxEvent> events);

    /**
     * Finds an event by ID reactively.
     *
     * @param id the event ID
     * @return a publisher emitting the event if found
     */
    Publisher<OutboxEvent> findById(OutboxEventId id);

    /**
     * Finds events matching a query reactively.
     *
     * @param query the query criteria
     * @return a publisher emitting matching events
     */
    Publisher<OutboxEvent> findByQuery(OutboxQuery query);

    /**
     * Updates an event reactively.
     *
     * @param event the event to update
     * @return a publisher that completes when the event is updated
     */
    Publisher<Void> update(OutboxEvent event);

    /**
     * Deletes an event by ID reactively.
     *
     * @param id the event ID
     * @return a publisher that completes when the event is deleted
     */
    Publisher<Void> delete(OutboxEventId id);

    /**
     * Counts events matching a query reactively.
     *
     * @param query the query criteria
     * @return a publisher emitting the count
     */
    Publisher<Long> count(OutboxQuery query);
}
