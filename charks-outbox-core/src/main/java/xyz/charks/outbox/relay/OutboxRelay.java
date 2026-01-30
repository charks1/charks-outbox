package xyz.charks.outbox.relay;

import xyz.charks.outbox.broker.BrokerConnector;
import xyz.charks.outbox.broker.PublishResult;
import xyz.charks.outbox.core.OutboxEvent;
import xyz.charks.outbox.core.OutboxQuery;
import xyz.charks.outbox.core.OutboxStatusFilter;
import xyz.charks.outbox.core.OutboxRepository;
import xyz.charks.outbox.exception.OutboxPublishException;
import xyz.charks.outbox.retry.RetryPolicy;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Orchestrator that polls the outbox table and publishes events to the message broker.
 *
 * <p>The relay runs as a background process that:
 * <ol>
 *   <li>Fetches pending events from the repository</li>
 *   <li>Publishes each event to the broker</li>
 *   <li>Updates event status (published or failed)</li>
 *   <li>Handles retries according to the configured policy</li>
 * </ol>
 *
 * <p>Key features:
 * <ul>
 *   <li>Virtual thread support for efficient I/O handling</li>
 *   <li>Row-level locking for safe clustered deployments</li>
 *   <li>Configurable retry policies</li>
 *   <li>Metrics callbacks for observability</li>
 * </ul>
 *
 * <p>Example setup:
 * <pre>{@code
 * OutboxRepository repository = new JdbcOutboxRepository(dataSource);
 * BrokerConnector connector = new KafkaBrokerConnector(kafkaProducer);
 * RelayConfig config = RelayConfig.builder()
 *     .batchSize(100)
 *     .pollingInterval(Duration.ofSeconds(1))
 *     .build();
 *
 * OutboxRelay relay = new OutboxRelay(repository, connector, config);
 * relay.start();
 *
 * // On shutdown
 * relay.stop();
 * }</pre>
 *
 * @see RelayConfig
 * @see RelayMetrics
 */
public final class OutboxRelay implements RelayLifecycle {

    private final OutboxRepository repository;
    private final BrokerConnector connector;
    private final RelayConfig config;
    private final RelayMetrics metrics;
    private final RetryPolicy retryPolicy;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicReference<ScheduledExecutorService> scheduler = new AtomicReference<>();
    private final AtomicReference<ExecutorService> executor = new AtomicReference<>();

    /**
     * Creates a new OutboxRelay with the specified components.
     *
     * @param repository the outbox event repository
     * @param connector the message broker connector
     * @param config relay configuration
     * @throws NullPointerException if any parameter is null
     */
    public OutboxRelay(OutboxRepository repository, BrokerConnector connector, RelayConfig config) {
        this(repository, connector, config, RelayMetrics.noop());
    }

    /**
     * Creates a new OutboxRelay with metrics support.
     *
     * @param repository the outbox event repository
     * @param connector the message broker connector
     * @param config relay configuration
     * @param metrics metrics callback handler
     * @throws NullPointerException if any parameter is null
     */
    public OutboxRelay(
            OutboxRepository repository,
            BrokerConnector connector,
            RelayConfig config,
            RelayMetrics metrics
    ) {
        this.repository = Objects.requireNonNull(repository, "Repository cannot be null");
        this.connector = Objects.requireNonNull(connector, "Connector cannot be null");
        this.config = Objects.requireNonNull(config, "Config cannot be null");
        this.metrics = Objects.requireNonNull(metrics, "Metrics cannot be null");
        this.retryPolicy = config.retryPolicy();
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        ExecutorService exec = config.useVirtualThreads()
                ? Executors.newVirtualThreadPerTaskExecutor()
                : Executors.newCachedThreadPool();
        executor.set(exec);

        ScheduledExecutorService sched = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "outbox-relay-scheduler");
            t.setDaemon(true);
            return t;
        });
        scheduler.set(sched);

        sched.scheduleWithFixedDelay(
                this::runPollingCycle,
                0,
                config.pollingInterval().toMillis(),
                TimeUnit.MILLISECONDS
        );

        metrics.onRelayStarted();
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        boolean graceful = true;
        ScheduledExecutorService sched = scheduler.getAndSet(null);
        if (sched != null) {
            sched.shutdown();
            try {
                if (!sched.awaitTermination(config.shutdownTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
                    sched.shutdownNow();
                    graceful = false;
                }
            } catch (InterruptedException e) {
                sched.shutdownNow();
                Thread.currentThread().interrupt();
                graceful = false;
            }
        }

        ExecutorService exec = executor.getAndSet(null);
        if (exec != null) {
            exec.shutdown();
            try {
                if (!exec.awaitTermination(config.shutdownTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
                    exec.shutdownNow();
                    graceful = false;
                }
            } catch (InterruptedException e) {
                exec.shutdownNow();
                Thread.currentThread().interrupt();
                graceful = false;
            }
        }

        metrics.onRelayStopped(graceful);
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public int processBatch() {
        return doProcessBatch();
    }

    private void runPollingCycle() {
        if (!running.get()) {
            return;
        }

        try {
            doProcessBatch();
        } catch (Exception e) {
            // Log error but continue polling
        }
    }

    private int doProcessBatch() {
        metrics.onPollingCycleStart();
        Instant fetchStart = Instant.now();

        OutboxQuery query = OutboxQuery.builder()
                .status(OutboxStatusFilter.PENDING)
                .limit(config.batchSize())
                .lockMode(config.lockMode())
                .maxRetryCount(retryPolicy.maxAttempts())
                .build();

        List<OutboxEvent> events = repository.find(query);
        Duration fetchDuration = Duration.between(fetchStart, Instant.now());
        metrics.onPollingCycleEnd(events.size(), fetchDuration);

        if (events.isEmpty()) {
            return 0;
        }

        Instant batchStart = Instant.now();
        int succeeded = 0;
        int failed = 0;

        for (OutboxEvent event : events) {
            try {
                boolean success = processEvent(event);
                if (success) {
                    succeeded++;
                } else {
                    failed++;
                }
            } catch (Exception e) {
                failed++;
            }
        }

        Duration batchDuration = Duration.between(batchStart, Instant.now());
        metrics.onBatchCompleted(events.size(), succeeded, failed, batchDuration);

        return events.size();
    }

    private boolean processEvent(OutboxEvent event) {
        Instant publishStart = Instant.now();

        try {
            PublishResult result = connector.publish(event);

            if (result.success()) {
                Instant publishedAt = result.publishedAt() != null ? result.publishedAt() : Instant.now();
                OutboxEvent published = event.markPublished(publishedAt);
                repository.update(published);

                Duration duration = Duration.between(publishStart, Instant.now());
                metrics.onEventPublished(published, duration);
                return true;
            } else {
                handleFailure(event, new OutboxPublishException(result.getError().orElse("Unknown error")));
                return false;
            }
        } catch (Exception e) {
            handleFailure(event, e);
            return false;
        }
    }

    private void handleFailure(OutboxEvent event, Throwable error) {
        metrics.onEventFailed(event, error);

        OutboxEvent failed = event.markFailed(error.getMessage() != null ? error.getMessage() : error.getClass().getName());

        if (retryPolicy.shouldRetry(failed) && retryPolicy.isRetryable(error)) {
            retryPolicy.nextRetryDelay(failed, error).ifPresent(delay -> {
                metrics.onEventRetryScheduled(failed.id(), failed.retryCount(), delay);
            });
            repository.update(failed);
        } else {
            metrics.onEventExhausted(failed, failed.retryCount(), error);
            repository.update(failed);
        }
    }
}
