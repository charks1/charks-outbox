package xyz.charks.outbox.r2dbc;

import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import xyz.charks.outbox.core.OutboxEvent;
import xyz.charks.outbox.core.OutboxEventId;
import xyz.charks.outbox.core.OutboxQuery;
import xyz.charks.outbox.core.OutboxStatus;
import xyz.charks.outbox.core.OutboxStatusFilter;

import java.util.List;

/**
 * Reactive repository interface for outbox events using Project Reactor.
 *
 * <p>This interface provides non-blocking operations for managing outbox events,
 * returning {@link Mono} and {@link Flux} types for reactive stream processing.
 *
 * <p>Example usage:
 * <pre>{@code
 * ReactiveOutboxRepository repository = ...;
 *
 * // Save an event reactively
 * repository.saveReactive(event)
 *     .doOnSuccess(e -> log.info("Saved event: {}", e.id()))
 *     .subscribe();
 *
 * // Find pending events
 * repository.findReactive(OutboxQuery.pending().limit(10))
 *     .doOnNext(e -> log.info("Found event: {}", e.id()))
 *     .subscribe();
 * }</pre>
 *
 * @see R2dbcOutboxRepository
 */
public interface ReactiveOutboxRepository {

    /**
     * Saves an outbox event reactively.
     *
     * @param event the event to save
     * @return a Mono emitting the saved event
     */
    Mono<OutboxEvent> saveReactive(OutboxEvent event);

    /**
     * Saves multiple outbox events reactively.
     *
     * @param events the events to save
     * @return a Flux emitting the saved events
     */
    Flux<OutboxEvent> saveAllReactive(List<OutboxEvent> events);

    /**
     * Finds an event by its ID reactively.
     *
     * @param id the event ID
     * @return a Mono emitting the event if found, or empty if not found
     */
    Mono<OutboxEvent> findByIdReactive(OutboxEventId id);

    /**
     * Finds events matching the query reactively.
     *
     * @param query the query criteria
     * @return a Flux emitting matching events
     */
    Flux<OutboxEvent> findReactive(OutboxQuery query);

    /**
     * Updates an event reactively.
     *
     * @param event the event with updated fields
     * @return a Mono emitting the updated event
     */
    Mono<OutboxEvent> updateReactive(OutboxEvent event);

    /**
     * Updates the status of multiple events reactively.
     *
     * @param ids the event IDs to update
     * @param status the new status
     * @return a Mono emitting the number of updated events
     */
    Mono<Integer> updateStatusReactive(List<OutboxEventId> ids, OutboxStatus status);

    /**
     * Deletes an event by its ID reactively.
     *
     * @param id the event ID
     * @return a Mono emitting true if deleted, false if not found
     */
    Mono<Boolean> deleteByIdReactive(OutboxEventId id);

    /**
     * Deletes events matching the query reactively.
     *
     * @param query the query criteria
     * @return a Mono emitting the number of deleted events
     */
    Mono<Integer> deleteReactive(OutboxQuery query);

    /**
     * Counts events by status filter reactively.
     *
     * @param statusFilter the status filter, or null for all events
     * @return a Mono emitting the count
     */
    Mono<Long> countReactive(@Nullable OutboxStatusFilter statusFilter);
}
