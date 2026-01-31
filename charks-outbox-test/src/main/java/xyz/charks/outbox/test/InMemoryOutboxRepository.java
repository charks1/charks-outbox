package xyz.charks.outbox.test;

import xyz.charks.outbox.core.OutboxEvent;
import xyz.charks.outbox.core.OutboxEventId;
import xyz.charks.outbox.core.OutboxQuery;
import xyz.charks.outbox.core.OutboxRepository;
import xyz.charks.outbox.core.OutboxStatus;
import xyz.charks.outbox.core.OutboxStatusFilter;
import xyz.charks.outbox.core.Pending;
import xyz.charks.outbox.core.Published;
import xyz.charks.outbox.core.Failed;
import xyz.charks.outbox.core.Archived;
import xyz.charks.outbox.exception.OutboxPersistenceException;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

/**
 * In-memory implementation of {@link OutboxRepository} for testing.
 *
 * <p>Provides a thread-safe, non-persistent repository suitable for unit tests.
 * All data is stored in memory and cleared when the instance is discarded.
 *
 * <p>Features:
 * <ul>
 *   <li>Thread-safe using ConcurrentHashMap</li>
 *   <li>Supports all query operations</li>
 *   <li>Tracks all saved events for assertions</li>
 *   <li>Reset capability for test isolation</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * InMemoryOutboxRepository repository = new InMemoryOutboxRepository();
 *
 * // Use in tests
 * repository.save(event);
 * assertThat(repository.findById(event.id())).isPresent();
 *
 * // Reset between tests
 * repository.clear();
 * }</pre>
 *
 * @see OutboxRepository
 */
public class InMemoryOutboxRepository implements OutboxRepository {

    private final Map<OutboxEventId, OutboxEvent> events = new ConcurrentHashMap<>();

    @Override
    public OutboxEvent save(OutboxEvent event) {
        Objects.requireNonNull(event, "Event cannot be null");
        events.put(event.id(), event);
        return event;
    }

    @Override
    public List<OutboxEvent> saveAll(List<OutboxEvent> eventList) {
        Objects.requireNonNull(eventList, "Events cannot be null");
        eventList.forEach(this::save);
        return eventList;
    }

    @Override
    public Optional<OutboxEvent> findById(OutboxEventId id) {
        Objects.requireNonNull(id, "ID cannot be null");
        return Optional.ofNullable(events.get(id));
    }

    @Override
    public List<OutboxEvent> find(OutboxQuery query) {
        Objects.requireNonNull(query, "Query cannot be null");

        Predicate<OutboxEvent> filter = buildFilter(query);

        return events.values().stream()
                .filter(filter)
                .sorted(Comparator.comparing(OutboxEvent::createdAt))
                .limit(query.limit())
                .toList();
    }

    @Override
    public OutboxEvent update(OutboxEvent event) {
        Objects.requireNonNull(event, "Event cannot be null");
        if (!events.containsKey(event.id())) {
            throw new OutboxPersistenceException("Event not found: " + event.id());
        }
        events.put(event.id(), event);
        return event;
    }

    @Override
    public int updateStatus(List<OutboxEventId> ids, OutboxStatus status) {
        Objects.requireNonNull(ids, "IDs cannot be null");
        Objects.requireNonNull(status, "Status cannot be null");

        int updated = 0;
        for (OutboxEventId id : ids) {
            if (events.computeIfPresent(id, (_, event) -> event.withStatus(status)) != null) {
                updated++;
            }
        }
        return updated;
    }

    @Override
    public boolean deleteById(OutboxEventId id) {
        Objects.requireNonNull(id, "ID cannot be null");
        return events.remove(id) != null;
    }

    @Override
    public int delete(OutboxQuery query) {
        Objects.requireNonNull(query, "Query cannot be null");

        Predicate<OutboxEvent> filter = buildFilter(query);

        List<OutboxEventId> toDelete = events.values().stream()
                .filter(filter)
                .sorted(Comparator.comparing(OutboxEvent::createdAt))
                .limit(query.limit())
                .map(OutboxEvent::id)
                .toList();

        toDelete.forEach(events::remove);
        return toDelete.size();
    }

    @Override
    public long count(@Nullable OutboxStatusFilter statusFilter) {
        if (statusFilter == null) {
            return events.size();
        }

        Predicate<OutboxEvent> filter = statusFilterPredicate(statusFilter);
        return events.values().stream().filter(filter).count();
    }

    /**
     * Returns all stored events.
     *
     * <p>Useful for test assertions.
     *
     * @return list of all events in insertion order
     */
    public List<OutboxEvent> findAll() {
        return new ArrayList<>(events.values());
    }

    /**
     * Clears all stored events.
     *
     * <p>Call this method between tests to ensure isolation.
     */
    public void clear() {
        events.clear();
    }

    /**
     * Returns the number of stored events.
     *
     * @return event count
     */
    public int size() {
        return events.size();
    }

    /**
     * Checks if the repository is empty.
     *
     * @return true if no events are stored
     */
    public boolean isEmpty() {
        return events.isEmpty();
    }

    private Predicate<OutboxEvent> buildFilter(OutboxQuery query) {
        Predicate<OutboxEvent> filter = _ -> true;

        OutboxStatusFilter statusFilter = query.statusFilter();
        if (statusFilter != null) {
            filter = filter.and(statusFilterPredicate(statusFilter));
        }

        if (!query.aggregateTypes().isEmpty()) {
            filter = filter.and(e -> query.aggregateTypes().contains(e.aggregateType()));
        }

        if (!query.topics().isEmpty()) {
            filter = filter.and(e -> query.topics().contains(e.topic()));
        }

        return filter;
    }

    private Predicate<OutboxEvent> statusFilterPredicate(OutboxStatusFilter statusFilter) {
        return switch (statusFilter) {
            case PENDING -> e -> e.status() instanceof Pending;
            case PUBLISHED -> e -> e.status() instanceof Published;
            case FAILED -> e -> e.status() instanceof Failed;
            case ARCHIVED -> e -> e.status() instanceof Archived;
            case RETRYABLE -> e -> e.status() instanceof Failed failed && failed.canRetry();
        };
    }
}
