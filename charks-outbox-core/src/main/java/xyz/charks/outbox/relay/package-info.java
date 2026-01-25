/**
 * Outbox relay orchestrator for processing and publishing pending events.
 *
 * <p>The relay component polls the outbox table for pending events and publishes
 * them to the configured message broker. Key features:
 * <ul>
 *   <li>Batch processing with configurable size</li>
 *   <li>Row-level locking to prevent duplicate processing in clustered deployments</li>
 *   <li>Pluggable retry policies with exponential backoff support</li>
 *   <li>Virtual thread execution for efficient I/O handling</li>
 * </ul>
 *
 * @see xyz.charks.outbox.relay.OutboxRelay
 * @see xyz.charks.outbox.relay.RelayConfig
 */
@NullMarked
package xyz.charks.outbox.relay;

import org.jspecify.annotations.NullMarked;
