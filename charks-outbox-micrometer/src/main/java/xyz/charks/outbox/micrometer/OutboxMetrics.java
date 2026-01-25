package xyz.charks.outbox.micrometer;

import io.micrometer.core.instrument.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.charks.outbox.core.OutboxEvent;
import xyz.charks.outbox.core.OutboxEventId;
import xyz.charks.outbox.relay.RelayMetrics;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Micrometer implementation of {@link RelayMetrics}.
 *
 * <p>This class provides metrics collection for the outbox relay using
 * Micrometer, supporting various monitoring systems like Prometheus,
 * Datadog, CloudWatch, etc.
 *
 * <p>Recorded metrics include:
 * <ul>
 *   <li>{@code outbox.events.published} - Counter of successfully published events</li>
 *   <li>{@code outbox.events.failed} - Counter of failed publish attempts</li>
 *   <li>{@code outbox.events.pending} - Gauge of pending events</li>
 *   <li>{@code outbox.relay.batch.duration} - Timer for batch processing duration</li>
 *   <li>{@code outbox.relay.publish.duration} - Timer for individual publish duration</li>
 * </ul>
 *
 * @see RelayMetrics
 */
public class OutboxMetrics implements RelayMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(OutboxMetrics.class);

    private static final String METRIC_PREFIX = "outbox";

    private final Counter publishedCounter;
    private final Counter failedCounter;
    private final Counter retriedCounter;
    private final Counter exhaustedCounter;
    private final AtomicLong pendingGauge;
    private final Timer batchDurationTimer;
    private final Timer publishDurationTimer;
    private final Timer fetchDurationTimer;
    private final DistributionSummary batchSizeSummary;

    /**
     * Creates a new OutboxMetrics instance.
     *
     * @param registry the Micrometer meter registry
     */
    public OutboxMetrics(MeterRegistry registry) {
        this(registry, Tags.empty());
    }

    /**
     * Creates a new OutboxMetrics instance with custom tags.
     *
     * @param registry the Micrometer meter registry
     * @param tags additional tags to apply to all metrics
     */
    public OutboxMetrics(MeterRegistry registry, Tags tags) {
        Objects.requireNonNull(registry, "registry");
        Objects.requireNonNull(tags, "tags");

        this.publishedCounter = Counter.builder(METRIC_PREFIX + ".events.published")
            .description("Number of successfully published outbox events")
            .tags(tags)
            .register(registry);

        this.failedCounter = Counter.builder(METRIC_PREFIX + ".events.failed")
            .description("Number of failed outbox event publish attempts")
            .tags(tags)
            .register(registry);

        this.retriedCounter = Counter.builder(METRIC_PREFIX + ".events.retried")
            .description("Number of outbox events scheduled for retry")
            .tags(tags)
            .register(registry);

        this.exhaustedCounter = Counter.builder(METRIC_PREFIX + ".events.exhausted")
            .description("Number of outbox events that exhausted all retry attempts")
            .tags(tags)
            .register(registry);

        this.pendingGauge = new AtomicLong(0);
        Gauge.builder(METRIC_PREFIX + ".events.pending", pendingGauge, AtomicLong::get)
            .description("Number of pending outbox events")
            .tags(tags)
            .register(registry);

        this.batchDurationTimer = Timer.builder(METRIC_PREFIX + ".relay.batch.duration")
            .description("Duration of outbox relay batch processing")
            .tags(tags)
            .register(registry);

        this.publishDurationTimer = Timer.builder(METRIC_PREFIX + ".relay.publish.duration")
            .description("Duration of individual event publish operations")
            .tags(tags)
            .register(registry);

        this.fetchDurationTimer = Timer.builder(METRIC_PREFIX + ".relay.fetch.duration")
            .description("Duration of fetching events from the database")
            .tags(tags)
            .register(registry);

        this.batchSizeSummary = DistributionSummary.builder(METRIC_PREFIX + ".relay.batch.size")
            .description("Size of processed batches")
            .tags(tags)
            .register(registry);

        LOG.debug("Initialized Micrometer outbox metrics");
    }

    @Override
    public void onEventPublished(OutboxEvent event, Duration duration) {
        publishedCounter.increment();
        publishDurationTimer.record(duration);
    }

    @Override
    public void onEventFailed(OutboxEvent event, Throwable error) {
        failedCounter.increment();
    }

    @Override
    public void onEventRetryScheduled(OutboxEventId eventId, int attemptNumber, Duration delayUntilRetry) {
        retriedCounter.increment();
    }

    @Override
    public void onEventExhausted(OutboxEvent event, int totalAttempts, Throwable lastError) {
        exhaustedCounter.increment();
    }

    @Override
    public void onBatchCompleted(int processed, int succeeded, int failed, Duration duration) {
        batchDurationTimer.record(duration);
        batchSizeSummary.record(processed);
    }

    @Override
    public void onPollingCycleEnd(int eventsFetched, Duration fetchDuration) {
        fetchDurationTimer.record(fetchDuration);
    }

    /**
     * Updates the pending events gauge.
     *
     * @param count the current number of pending events
     */
    public void updatePendingCount(long count) {
        pendingGauge.set(count);
    }
}
