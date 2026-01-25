/**
 * Jackson-based JSON serialization for outbox events.
 *
 * <p>This module provides a JSON serializer implementation using Jackson,
 * suitable for most use cases where human-readable payloads are preferred.
 *
 * <h2>Features</h2>
 * <ul>
 *   <li>Full Java 8+ date/time support via JavaTimeModule</li>
 *   <li>Configurable ObjectMapper for custom serialization settings</li>
 *   <li>Pretty printing option for debugging</li>
 *   <li>ISO-8601 date format by default</li>
 * </ul>
 *
 * <h2>Usage Example</h2>
 * <pre>{@code
 * Serializer<Object> serializer = JacksonSerializer.create();
 *
 * // Serialize
 * byte[] payload = serializer.serialize(event);
 *
 * // Deserialize
 * MyEvent restored = serializer.deserialize(payload, MyEvent.class);
 * }</pre>
 *
 * @see xyz.charks.outbox.serializer.json.JacksonSerializer
 */
@NullMarked
package xyz.charks.outbox.serializer.json;

import org.jspecify.annotations.NullMarked;
