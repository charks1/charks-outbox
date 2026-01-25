package xyz.charks.outbox.quarkus;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.charks.outbox.relay.OutboxRelay;

/**
 * Quarkus lifecycle management for the outbox relay.
 *
 * <p>This class automatically starts the relay when the application starts
 * and stops it gracefully during shutdown.
 */
@ApplicationScoped
public class OutboxLifecycle {

    private static final Logger LOG = LoggerFactory.getLogger(OutboxLifecycle.class);

    @Inject
    Instance<OutboxRelay> relayInstance;

    @Inject
    OutboxConfiguration config;

    /**
     * Starts the outbox relay when the application starts.
     *
     * @param event the startup event
     */
    void onStart(@Observes StartupEvent event) {
        if (!config.enabled()) {
            LOG.debug("Outbox relay is disabled, skipping startup");
            return;
        }

        if (relayInstance.isUnsatisfied()) {
            LOG.debug("OutboxRelay not available, skipping startup");
            return;
        }

        OutboxRelay relay = relayInstance.get();
        if (relay != null) {
            relay.start();
            LOG.info("Started outbox relay");
        }
    }

    /**
     * Stops the outbox relay during shutdown.
     *
     * @param event the shutdown event
     */
    void onStop(@Observes ShutdownEvent event) {
        if (relayInstance.isUnsatisfied()) {
            return;
        }

        OutboxRelay relay = relayInstance.get();
        if (relay != null) {
            LOG.info("Stopping outbox relay...");
            relay.stop();
            LOG.info("Outbox relay stopped");
        }
    }
}
