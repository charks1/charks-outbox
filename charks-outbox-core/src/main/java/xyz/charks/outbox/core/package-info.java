/**
 * Core domain model for the Transactional Outbox pattern.
 *
 * <p>This package contains the fundamental building blocks of the outbox library:
 * <ul>
 *   <li>{@link xyz.charks.outbox.core.OutboxEvent} - The main entity representing an outbox event</li>
 *   <li>{@link xyz.charks.outbox.core.OutboxEventId} - Type-safe event identifier</li>
 *   <li>{@link xyz.charks.outbox.core.AggregateId} - Identifier for domain aggregates</li>
 *   <li>{@link xyz.charks.outbox.core.EventType} - Type discriminator for events</li>
 *   <li>{@link xyz.charks.outbox.core.OutboxStatus} - Sealed hierarchy representing event lifecycle states</li>
 * </ul>
 *
 * <p>The core module has zero runtime dependencies beyond JSpecify annotations,
 * making it suitable for any Java application regardless of framework choice.
 *
 * @see xyz.charks.outbox.core.OutboxEvent
 * @see xyz.charks.outbox.core.OutboxRepository
 */
@NullMarked
package xyz.charks.outbox.core;

import org.jspecify.annotations.NullMarked;
