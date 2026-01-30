package xyz.charks.outbox.serializer.protobuf;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.charks.outbox.exception.OutboxSerializationException;
import xyz.charks.outbox.serializer.Serializer;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Protocol Buffers implementation of {@link Serializer}.
 *
 * <p>This serializer handles Protobuf Message objects, providing efficient
 * binary serialization with schema evolution support.
 *
 * @see Serializer
 */
public class ProtobufSerializer implements Serializer<Object> {

    private static final Logger LOG = LoggerFactory.getLogger(ProtobufSerializer.class);
    private static final String CONTENT_TYPE = "application/x-protobuf";

    private final Map<Class<?>, Parser<?>> parserCache = new ConcurrentHashMap<>();

    /**
     * Creates a new Protobuf serializer.
     */
    public ProtobufSerializer() {
        LOG.debug("Initialized Protobuf serializer");
    }

    @Override
    public byte[] serialize(Object payload) {
        Objects.requireNonNull(payload, "payload");

        return switch (payload) {
            case Message message -> message.toByteArray();
            case byte[] bytes -> bytes;
            default -> throw new OutboxSerializationException(
                "Unsupported payload type: " + payload.getClass().getName() +
                ". Expected com.google.protobuf.Message or byte[]");
        };
    }

    @Override
    @SuppressWarnings("unchecked")
    public <R extends Object> R deserialize(byte[] data, Class<R> type) {
        Objects.requireNonNull(data, "data");
        Objects.requireNonNull(type, "type");

        if (byte[].class.equals(type)) {
            return (R) data;
        }

        if (!Message.class.isAssignableFrom(type)) {
            throw new OutboxSerializationException(
                "Unsupported target type: " + type.getName() +
                ". Expected com.google.protobuf.Message subclass or byte[]");
        }

        try {
            Parser<?> parser = getParser(type);
            return (R) parser.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
            throw new OutboxSerializationException("Failed to deserialize Protobuf payload", e);
        }
    }

    @Override
    public String contentType() {
        return CONTENT_TYPE;
    }

    @SuppressWarnings("unchecked")
    private <T> Parser<T> getParser(Class<T> type) {
        return (Parser<T>) parserCache.computeIfAbsent(type, this::createParser);
    }

    private Parser<?> createParser(Class<?> type) {
        try {
            Method parserMethod = type.getMethod("parser");
            return (Parser<?>) parserMethod.invoke(null);
        } catch (ReflectiveOperationException e) {
            throw new OutboxSerializationException(
                "Failed to get parser for Protobuf type: " + type.getName() +
                ". Ensure the class has a static parser() method.", e);
        }
    }
}
