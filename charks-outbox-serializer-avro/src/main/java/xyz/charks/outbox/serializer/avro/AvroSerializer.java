package xyz.charks.outbox.serializer.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.charks.outbox.exception.OutboxSerializationException;
import xyz.charks.outbox.serializer.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Objects;

/**
 * Apache Avro implementation of {@link Serializer}.
 *
 * <p>This serializer supports both specific Avro records (generated from schemas)
 * and generic records using provided schemas.
 *
 * @see Serializer
 */
public class AvroSerializer implements Serializer<Object> {

    private static final Logger LOG = LoggerFactory.getLogger(AvroSerializer.class);
    private static final String CONTENT_TYPE = "application/avro";

    /**
     * Creates a new Avro serializer.
     */
    public AvroSerializer() {
        LOG.debug("Initialized Avro serializer");
    }

    @Override
    public byte[] serialize(Object payload) {
        Objects.requireNonNull(payload, "payload");

        try {
            return switch (payload) {
                case SpecificRecordBase specificRecord -> serializeSpecific(specificRecord);
                case GenericRecord genericRecord -> serializeGeneric(genericRecord);
                case byte[] bytes -> bytes;
                default -> throw new OutboxSerializationException(
                    "Unsupported payload type: " + payload.getClass().getName() +
                    ". Expected SpecificRecordBase, GenericRecord, or byte[]");
            };
        } catch (IOException e) {
            throw new OutboxSerializationException("Failed to serialize Avro payload", e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <R extends Object> R deserialize(byte[] data, Class<R> type) {
        Objects.requireNonNull(data, "data");
        Objects.requireNonNull(type, "type");

        try {
            if (SpecificRecordBase.class.isAssignableFrom(type)) {
                return (R) deserializeSpecific(data, (Class<? extends SpecificRecordBase>) type);
            } else if (byte[].class.equals(type)) {
                return (R) data;
            } else {
                throw new OutboxSerializationException(
                    "Unsupported target type: " + type.getName() +
                    ". Expected SpecificRecordBase subclass or byte[]");
            }
        } catch (IOException e) {
            throw new OutboxSerializationException("Failed to deserialize Avro payload", e);
        }
    }

    @Override
    public String contentType() {
        return CONTENT_TYPE;
    }

    private byte[] serializeSpecific(SpecificRecordBase avroRecord) throws IOException {
        Schema schema = avroRecord.getSchema();
        DatumWriter<SpecificRecordBase> writer = new SpecificDatumWriter<>(schema);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(avroRecord, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    private byte[] serializeGeneric(GenericRecord avroRecord) throws IOException {
        Schema schema = avroRecord.getSchema();
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(avroRecord, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    private <T extends SpecificRecordBase> T deserializeSpecific(byte[] data, Class<T> type) throws IOException {
        try {
            T instance = type.getDeclaredConstructor().newInstance();
            Schema schema = instance.getSchema();
            DatumReader<T> reader = new SpecificDatumReader<>(schema);
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
            return reader.read(instance, decoder);
        } catch (ReflectiveOperationException e) {
            throw new IOException("Failed to instantiate Avro record: " + type.getName(), e);
        }
    }

    /**
     * Deserializes data to a generic record using the provided schema.
     *
     * @param data the serialized data
     * @param schema the Avro schema
     * @return the deserialized generic record
     * @throws OutboxSerializationException if deserialization fails
     */
    public GenericRecord deserializeGeneric(byte[] data, Schema schema) {
        Objects.requireNonNull(data, "data");
        Objects.requireNonNull(schema, "schema");

        try {
            DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
            return reader.read(null, decoder);
        } catch (IOException e) {
            throw new OutboxSerializationException("Failed to deserialize Avro generic record", e);
        }
    }
}
