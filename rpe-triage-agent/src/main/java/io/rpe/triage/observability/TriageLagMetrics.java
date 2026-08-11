package io.rpe.triage.observability;

import io.rpe.triage.config.KafkaTriageConfig;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.rpe.triage.config.BoundaryHandler;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@code triage_consumer_lag{group}} — locked signal (ADR-15 §5.3).
 *
 * Lag rising while the CB is open/slow is the §6.1 detection pair: provider
 * degradation shows here before it shows as errors. Total lag across all
 * {@code payment.alerts} partitions for the triage group; 30s refresh, bounded waits.
 *
 * <p>Poll failures increment {@code triage.consumer.lag.poll_failures} (the Bundle-1
 * false-healthy lesson, same as {@code rpe.dlt.depth.poll_failures}): a failing poll freezes
 * the lag gauge at its last value — 0 at boot — and the {@code @BoundaryHandler} absorbs the
 * error, so without the counter a dead poller reads as "no lag".
 */
@Component
@BoundaryHandler("observability-poll-must-not-throw into the scheduling thread; a lag refresh failure logs and the next run retries")
public class TriageLagMetrics {

    private static final Logger log = LoggerFactory.getLogger(TriageLagMetrics.class);

    private static final String SOURCE_TOPIC      = "payment.alerts";
    private static final long   ADMIN_TIMEOUT_SEC = 5;

    private final Admin admin;
    private final AtomicLong lag = new AtomicLong(0);
    private final Counter pollFailures;

    public TriageLagMetrics(Admin admin, MeterRegistry meterRegistry) {
        this.admin = admin;
        Gauge.builder("triage.consumer.lag", lag, AtomicLong::get)
                .tag("group", KafkaTriageConfig.CONSUMER_GROUP)
                .description("Total uncommitted payment.alerts messages for the triage group")
                .register(meterRegistry);
        this.pollFailures = Counter.builder("triage.consumer.lag.poll_failures")
                .tag("group", KafkaTriageConfig.CONSUMER_GROUP)
                .description("Failed lag polls; while rising, triage.consumer.lag is stale")
                .register(meterRegistry);
    }

    @Scheduled(fixedDelay = 30_000, initialDelay = 30_000)
    public void refresh() {
        try {
            Map<TopicPartition, OffsetAndMetadata> committed = admin
                    .listConsumerGroupOffsets(KafkaTriageConfig.CONSUMER_GROUP)
                    .partitionsToOffsetAndMetadata()
                    .get(ADMIN_TIMEOUT_SEC, TimeUnit.SECONDS);

            Map<TopicPartition, OffsetSpec> latestReq = new HashMap<>();
            committed.keySet().stream()
                    .filter(tp -> SOURCE_TOPIC.equals(tp.topic()))
                    .forEach(tp -> latestReq.put(tp, OffsetSpec.latest()));
            if (latestReq.isEmpty()) {
                lag.set(0);
                return;
            }

            var latest = admin.listOffsets(latestReq).all().get(ADMIN_TIMEOUT_SEC, TimeUnit.SECONDS);

            long total = 0;
            for (var entry : latestReq.entrySet()) {
                TopicPartition tp = entry.getKey();
                long end = latest.get(tp).offset();
                long cmt = committed.get(tp) == null ? 0 : committed.get(tp).offset();
                total += Math.max(0, end - cmt);
            }
            lag.set(total);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            pollFailures.increment();
            log.warn("Triage lag query failed: {}", e.getMessage());
        }
    }
}
