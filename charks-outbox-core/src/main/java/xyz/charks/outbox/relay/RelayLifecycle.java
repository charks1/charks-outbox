package xyz.charks.outbox.relay;

/**
 * Lifecycle management interface for the outbox relay.
 *
 * <p>Controls starting and stopping the relay's background processing.
 * Implementations should be thread-safe and support multiple start/stop cycles.
 *
 * <p>Example usage:
 * <pre>{@code
 * RelayLifecycle relay = new OutboxRelay(repository, connector, config);
 *
 * // Start processing
 * relay.start();
 *
 * // ... application runs ...
 *
 * // Graceful shutdown
 * relay.stop();
 * }</pre>
 *
 * <p>For Spring Boot integration, this interface can be used with
 * {@code SmartLifecycle} for automatic startup and shutdown.
 */
public interface RelayLifecycle {

    /**
     * Starts the relay's background processing.
     *
     * <p>After this method returns, the relay will begin polling
     * the outbox table and publishing events.
     *
     * <p>Calling start on an already running relay has no effect.
     *
     * @throws IllegalStateException if the relay cannot be started
     */
    void start();

    /**
     * Stops the relay's background processing.
     *
     * <p>This method initiates a graceful shutdown:
     * <ol>
     *   <li>Stops accepting new batches</li>
     *   <li>Waits for in-flight events to complete (up to shutdown timeout)</li>
     *   <li>Releases resources</li>
     * </ol>
     *
     * <p>Calling stop on an already stopped relay has no effect.
     */
    void stop();

    /**
     * Checks if the relay is currently running.
     *
     * @return true if the relay is processing events
     */
    boolean isRunning();

    /**
     * Performs a single processing cycle synchronously.
     *
     * <p>Useful for testing or manual trigger scenarios.
     * Does not start the background polling loop.
     *
     * @return the number of events processed
     */
    int processBatch();
}
