package xyz.charks.outbox.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.types.Binary;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.charks.outbox.core.*;
import xyz.charks.outbox.exception.OutboxPersistenceException;

import java.time.Instant;
import java.util.*;

/**
 * MongoDB implementation of {@link OutboxRepository}.
 *
 * <p>This implementation stores outbox events in a MongoDB collection,
 * providing document-based storage with support for distributed locking.
 *
 * @see MongoOutboxConfig
 */
public class MongoOutboxRepository implements OutboxRepository {

    private static final Logger LOG = LoggerFactory.getLogger(MongoOutboxRepository.class);

    private static final String FIELD_ID = "_id";
    private static final String FIELD_AGGREGATE_TYPE = "aggregateType";
    private static final String FIELD_AGGREGATE_ID = "aggregateId";
    private static final String FIELD_EVENT_TYPE = "eventType";
    private static final String FIELD_TOPIC = "topic";
    private static final String FIELD_PARTITION_KEY = "partitionKey";
    private static final String FIELD_PAYLOAD = "payload";
    private static final String FIELD_HEADERS = "headers";
    private static final String FIELD_STATUS = "status";
    private static final String FIELD_RETRY_COUNT = "retryCount";
    private static final String FIELD_LAST_ERROR = "lastError";
    private static final String FIELD_CREATED_AT = "createdAt";
    private static final String FIELD_PROCESSED_AT = "processedAt";

    private final MongoCollection<Document> collection;

    /**
     * Creates a new MongoDB outbox repository.
     *
     * @param config the repository configuration
     */
    public MongoOutboxRepository(MongoOutboxConfig config) {
        Objects.requireNonNull(config, "config");
        MongoDatabase database = config.client().getDatabase(config.database());
        this.collection = database.getCollection(config.collection());
        LOG.info("Initialized MongoDB outbox repository with database '{}' and collection '{}'",
            config.database(), config.collection());
    }

    @Override
    public OutboxEvent save(OutboxEvent event) {
        Objects.requireNonNull(event, "event");
        Document doc = toDocument(event);
        collection.insertOne(doc);
        LOG.debug("Saved event {}", event.id());
        return event;
    }

    @Override
    public List<OutboxEvent> saveAll(List<OutboxEvent> events) {
        Objects.requireNonNull(events, "events");
        if (events.isEmpty()) {
            return List.of();
        }
        List<Document> docs = events.stream().map(this::toDocument).toList();
        collection.insertMany(docs);
        LOG.debug("Saved {} events", events.size());
        return events;
    }

    @Override
    public Optional<OutboxEvent> findById(OutboxEventId id) {
        Objects.requireNonNull(id, "id");
        Document doc = collection.find(Filters.eq(FIELD_ID, id.value().toString())).first();
        return Optional.ofNullable(doc).map(this::fromDocument);
    }

    @Override
    public List<OutboxEvent> find(OutboxQuery query) {
        Objects.requireNonNull(query, "query");

        var filters = new ArrayList<org.bson.conversions.Bson>();

        if (query.statusFilter() != null) {
            filters.add(Filters.eq(FIELD_STATUS, query.statusFilter().name()));
        }

        if (!query.aggregateTypes().isEmpty()) {
            filters.add(Filters.in(FIELD_AGGREGATE_TYPE, query.aggregateTypes()));
        }

        if (query.createdAfter() != null) {
            filters.add(Filters.gte(FIELD_CREATED_AT, Date.from(query.createdAfter())));
        }

        if (query.createdBefore() != null) {
            filters.add(Filters.lte(FIELD_CREATED_AT, Date.from(query.createdBefore())));
        }

        var filter = filters.isEmpty() ? new Document() : Filters.and(filters);

        return collection.find(filter)
            .limit(query.limit())
            .map(this::fromDocument)
            .into(new ArrayList<>());
    }

    @Override
    public OutboxEvent update(OutboxEvent event) {
        Objects.requireNonNull(event, "event");
        Document doc = toDocument(event);
        var result = collection.replaceOne(Filters.eq(FIELD_ID, event.id().value().toString()), doc);
        if (result.getMatchedCount() == 0) {
            throw new OutboxPersistenceException("Event not found: " + event.id());
        }
        LOG.debug("Updated event {}", event.id());
        return event;
    }

    @Override
    public int updateStatus(List<OutboxEventId> ids, OutboxStatus status) {
        Objects.requireNonNull(ids, "ids");
        Objects.requireNonNull(status, "status");
        if (ids.isEmpty()) {
            return 0;
        }
        List<String> idStrings = ids.stream().map(id -> id.value().toString()).toList();

        Document updateFields = new Document(FIELD_STATUS, status.name());

        switch (status) {
            case Failed f -> {
                updateFields.append(FIELD_LAST_ERROR, f.error());
                updateFields.append(FIELD_RETRY_COUNT, f.attemptCount());
                updateFields.append(FIELD_PROCESSED_AT, Date.from(f.failedAt()));
            }
            case Published p -> {
                updateFields.append(FIELD_PROCESSED_AT, Date.from(p.publishedAt()));
            }
            case Archived a -> {
                updateFields.append(FIELD_PROCESSED_AT, Date.from(a.archivedAt()));
                updateFields.append(FIELD_LAST_ERROR, a.reason());
            }
            default -> {
                // Pending: no additional fields
            }
        }

        var result = collection.updateMany(
            Filters.in(FIELD_ID, idStrings),
            new Document("$set", updateFields)
        );
        LOG.debug("Updated status to {} for {} events", status.name(), result.getModifiedCount());
        return (int) result.getModifiedCount();
    }

    @Override
    public boolean deleteById(OutboxEventId id) {
        Objects.requireNonNull(id, "id");
        var result = collection.deleteOne(Filters.eq(FIELD_ID, id.value().toString()));
        LOG.debug("Deleted event {}", id);
        return result.getDeletedCount() > 0;
    }

    @Override
    public int delete(OutboxQuery query) {
        Objects.requireNonNull(query, "query");

        var filters = new ArrayList<org.bson.conversions.Bson>();

        if (query.statusFilter() != null) {
            filters.add(Filters.eq(FIELD_STATUS, query.statusFilter().name()));
        }

        var filter = filters.isEmpty() ? new Document() : Filters.and(filters);
        var result = collection.deleteMany(filter);
        LOG.debug("Deleted {} events", result.getDeletedCount());
        return (int) result.getDeletedCount();
    }

    @Override
    public long count(@Nullable OutboxStatusFilter statusFilter) {
        if (statusFilter == null) {
            return collection.countDocuments();
        }
        return collection.countDocuments(Filters.eq(FIELD_STATUS, statusFilter.name()));
    }

    private Document toDocument(OutboxEvent event) {
        Document doc = new Document();
        doc.put(FIELD_ID, event.id().value().toString());
        doc.put(FIELD_AGGREGATE_TYPE, event.aggregateType());
        doc.put(FIELD_AGGREGATE_ID, event.aggregateId().value());
        doc.put(FIELD_EVENT_TYPE, event.eventType().value());
        doc.put(FIELD_TOPIC, event.topic());
        doc.put(FIELD_PARTITION_KEY, event.partitionKey());
        doc.put(FIELD_PAYLOAD, new Binary(event.payload()));
        doc.put(FIELD_HEADERS, event.headers());
        doc.put(FIELD_STATUS, event.status().name());
        doc.put(FIELD_RETRY_COUNT, event.retryCount());
        doc.put(FIELD_LAST_ERROR, event.lastError());
        doc.put(FIELD_CREATED_AT, Date.from(event.createdAt()));
        if (event.processedAt() != null) {
            doc.put(FIELD_PROCESSED_AT, Date.from(event.processedAt()));
        }
        return doc;
    }

    private OutboxEvent fromDocument(Document doc) {
        String statusStr = doc.getString(FIELD_STATUS);
        Integer retryCount = doc.getInteger(FIELD_RETRY_COUNT, 0);
        Date createdAtDate = doc.getDate(FIELD_CREATED_AT);
        Date processedAtDate = doc.getDate(FIELD_PROCESSED_AT);
        Instant createdAt = createdAtDate != null ? createdAtDate.toInstant() : Instant.now();
        Instant processedAt = processedAtDate != null ? processedAtDate.toInstant() : null;

        OutboxStatus status = switch (statusStr) {
            case "PENDING" -> Pending.at(createdAt);
            case "PUBLISHED" -> Published.at(processedAt != null ? processedAt : Instant.now());
            case "FAILED" -> {
                String error = doc.getString(FIELD_LAST_ERROR);
                yield new Failed(
                    error != null ? error : "Unknown error",
                    retryCount,
                    processedAt != null ? processedAt : Instant.now()
                );
            }
            case "ARCHIVED" -> Archived.at(processedAt != null ? processedAt : Instant.now());
            default -> Pending.create();
        };

        Binary payloadBinary = doc.get(FIELD_PAYLOAD, Binary.class);
        byte[] payload = payloadBinary != null ? payloadBinary.getData() : new byte[0];

        @SuppressWarnings("unchecked")
        Map<String, String> headers = doc.get(FIELD_HEADERS, Map.class);

        return OutboxEvent.builder()
            .id(new OutboxEventId(UUID.fromString(doc.getString(FIELD_ID))))
            .aggregateType(doc.getString(FIELD_AGGREGATE_TYPE))
            .aggregateId(new AggregateId(doc.getString(FIELD_AGGREGATE_ID)))
            .eventType(new EventType(doc.getString(FIELD_EVENT_TYPE)))
            .topic(doc.getString(FIELD_TOPIC))
            .partitionKey(doc.getString(FIELD_PARTITION_KEY))
            .payload(payload)
            .headers(headers != null ? headers : Map.of())
            .status(status)
            .retryCount(retryCount)
            .lastError(doc.getString(FIELD_LAST_ERROR))
            .createdAt(createdAt)
            .processedAt(processedAt)
            .build();
    }
}
