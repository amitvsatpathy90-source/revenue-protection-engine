package io.rpe.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.rpe.config.BoundaryHandler;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@code rpe.dlt.depth{topic=...}} — message depth of the DLTs this service solely writes.
 *
 * Per ADR-23, depth is owned per-writer: detection monitors {@code payment.events.DLT} (and its
 * re-drive parking topic), alert-service monitors {@code payment.alerts.DLT}, triage monitors
 * {@code payment.alerts.triage.DLT} — each a per-service copy. A service never polls a DLT it does
 * not own.
 *
 * A growing DLT is the primary signal that events are being rejected (poison messages,
 * validation failures, gate contract violations). A non-empty {@code *.parked} topic is louder
 * still: the operator re-drive tool exhausted its attempt budget and gave up — manual decision
 * required (ADR-23 decision tree). Without these gauges, DLT routing is a write-only black hole
 * discovered during incident review.
 *
 * Depth = Σ per partition (latest offset − earliest offset); retention-expired messages
 * fall out of the count naturally. Tag cardinality is bounded: exactly the topics below.
 *
 * All admin waits are bounded (5s) — a hung broker delays, never wedges, the shared
 * scheduling thread.
 */
@Component
@BoundaryHandler("observability-poll-must-not-throw into the scheduling thread; a metric refresh failure logs and the next scheduled run retries")
public class DltDepthMetrics {

    private static final Logger log = LoggerFactory.getLogger(DltDepthMetrics.class);

    // Detection is the sole writer of payment.events.DLT (ADR-17/18). The .parked topic is where
    // the re-drive tool sends events that re-poisoned past the attempt cap (ADR-23).
    private static final List<String> DLT_TOPICS =
            List.of("payment.events.DLT", "payment.events.DLT.parked");
    private static final long ADMIN_TIMEOUT_SEC = 5;

    // The shared, transport-authenticated Admin bean (KafkaConfig#rpeAdminClient) — NEVER a
    // locally-created Admin from kafkaAdmin.getConfigurationProperties() alone: that map carries
    // no SASL creds (they are assembled in code, ADR-20), so a local client silently freezes
    // every gauge below at 0 in a SASL stack. Lifecycle (bounded close) is owned by the config.
    private final Admin admin;
    private final Map<String, AtomicLong> depthByTopic = new ConcurrentHashMap<>();
    private final Map<String, Counter> pollFailuresByTopic = new ConcurrentHashMap<>();

    public DltDepthMetrics(Admin admin, MeterRegistry meterRegistry) {
        this.admin = admin;
        for (String topic : DLT_TOPICS) {
            AtomicLong holder = new AtomicLong(0);
            depthByTopic.put(topic, holder);
            Gauge.builder("rpe.dlt.depth", holder, AtomicLong::get)
                    .tag("topic", topic)
                    .description("Messages currently retained on the dead-letter topic")
                    .register(meterRegistry);
            // Meta-signal for the gauge above: while this counter rises, the depth gauge is NOT
            // refreshing — it is frozen at its last value (0 at boot, i.e. false-healthy). First
            // suspects: admin transport creds (ADR-20) or a missing DESCRIBE ACL on the .parked
            // topic (ADR-23). Backs the RpeDltDepthPollFailing alert rule.
            pollFailuresByTopic.put(topic, Counter.builder("rpe.dlt.depth.poll_failures")
                    .tag("topic", topic)
                    .description("Failed depth polls; while rising, rpe.dlt.depth for this topic is stale")
                    .register(meterRegistry));
        }
    }

    @Scheduled(fixedDelay = 30_000, initialDelay = 20_000)
    public void refresh() {
        for (String topic : DLT_TOPICS) {
            try {
                long depth = queryDepth(topic);
                long previous = depthByTopic.get(topic).getAndSet(depth);
                if (depth > previous) {
                    log.warn("DLT depth rising on {}: {} -> {} message(s)", topic, previous, depth);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                    // Topic not created yet (fresh lab stack) — depth is legitimately 0
                    depthByTopic.get(topic).set(0);
                } else {
                    pollFailuresByTopic.get(topic).increment();
                    log.warn("DLT depth query failed for {}: {}", topic, e.getMessage());
                }
            } catch (Exception e) {
                pollFailuresByTopic.get(topic).increment();
                log.warn("DLT depth query failed for {}: {}", topic, e.getMessage());
            }
        }
    }

    private long queryDepth(String topic) throws Exception {
        var description = admin.describeTopics(List.of(topic))
                .topicNameValues().get(topic).get(ADMIN_TIMEOUT_SEC, TimeUnit.SECONDS);

        Map<TopicPartition, OffsetSpec> earliestReq = new HashMap<>();
        Map<TopicPartition, OffsetSpec> latestReq   = new HashMap<>();
        for (TopicPartitionInfo p : description.partitions()) {
            TopicPartition tp = new TopicPartition(topic, p.partition());
            earliestReq.put(tp, OffsetSpec.earliest());
            latestReq.put(tp, OffsetSpec.latest());
        }

        var earliest = admin.listOffsets(earliestReq).all().get(ADMIN_TIMEOUT_SEC, TimeUnit.SECONDS);
        var latest   = admin.listOffsets(latestReq).all().get(ADMIN_TIMEOUT_SEC, TimeUnit.SECONDS);

        long depth = 0;
        for (TopicPartition tp : earliestReq.keySet()) {
            depth += latest.get(tp).offset() - earliest.get(tp).offset();
        }
        return depth;
    }
}
