/**
 * Retry policy implementations for failed event processing.
 *
 * <p>This package provides pluggable retry strategies:
 * <ul>
 *   <li>{@link xyz.charks.outbox.retry.ExponentialBackoff} - Exponential delay with jitter</li>
 *   <li>{@link xyz.charks.outbox.retry.FixedDelay} - Constant delay between retries</li>
 *   <li>{@link xyz.charks.outbox.retry.NoRetry} - Single attempt only</li>
 * </ul>
 *
 * @see xyz.charks.outbox.retry.RetryPolicy
 */
@NullMarked
package xyz.charks.outbox.retry;

import org.jspecify.annotations.NullMarked;
