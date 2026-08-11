package io.rpe.consumer;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Graceful-shutdown drain of the per-account lanes (ADR-22).
 *
 * <b>Phase ordering is the whole point.</b> Spring stops {@link SmartLifecycle} beans in
 * DESCENDING phase order. Spring Kafka listener containers stop at
 * {@link AbstractMessageListenerContainer#DEFAULT_PHASE} ({@code Integer.MAX_VALUE - 100}).
 * This bean sits at {@code DEFAULT_PHASE - 10}, so it stops strictly AFTER the consumer
 * container — i.e. once ingestion has ceased and no new lane task can be submitted. That is
 * the precondition for a clean drain: we await only the tasks already in flight.
 *
 * Stop order overall: web drain → Kafka container stop → THIS (lane drain) → outbox flush
 * → pool shutdown. See {@code docs/adrs/lifecycle-shutdown.md}.
 *
 * Bounded: lanes are drained within {@link #DRAIN_TIMEOUT}. A timeout is not data loss — the
 * Kafka offsets of un-acked in-flight events stay uncommitted and are redelivered on restart
 * (dedup + deterministic alert_id make replay safe, ADR-13). It IS surfaced loudly via
 * {@code rpe.shutdown.forced{component=lanes}} so a chronically slow drain is visible.
 */
@Component
public class LaneDrainLifecycle implements SmartLifecycle {

    /** One rung below the Kafka container: stop AFTER ingestion stops (see class doc). */
    static final int PHASE = AbstractMessageListenerContainer.DEFAULT_PHASE - 10;

    private static final Logger   log           = LoggerFactory.getLogger(LaneDrainLifecycle.class);
    private static final Duration DRAIN_TIMEOUT = Duration.ofSeconds(5);

    private final LaneExecutorService lanes;
    private final MeterRegistry       meterRegistry;

    private volatile boolean running;

    public LaneDrainLifecycle(LaneExecutorService lanes, MeterRegistry meterRegistry) {
        this.lanes         = lanes;
        this.meterRegistry = meterRegistry;
    }

    @Override
    public void start() {
        running = true;
    }

    @Override
    public void stop() {
        if (!running) {
            return;
        }
        running = false;

        log.info("Graceful shutdown: draining per-account lanes (≤{}s)", DRAIN_TIMEOUT.toSeconds());
        long startNanos = System.nanoTime();
        LaneExecutorService.DrainResult result = lanes.drainAll(DRAIN_TIMEOUT);

        meterRegistry.timer("rpe.shutdown.drain.duration", "component", "lanes")
                .record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
        meterRegistry.counter("rpe.shutdown.tasks.drained", "component", "lanes")
                .increment(result.drained());

        if (result.forced()) {
            meterRegistry.counter("rpe.shutdown.forced", "component", "lanes").increment();
            log.warn("Lane drain timed out: {}/{} lanes drained within {}s — remaining tasks "
                    + "abandoned; their offsets stay uncommitted and redeliver on restart (dedup-safe)",
                    result.drained(), result.lanes(), DRAIN_TIMEOUT.toSeconds());
        } else {
            log.info("Lane drain complete: {}/{} lanes drained cleanly", result.drained(), result.lanes());
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return PHASE;
    }
}
