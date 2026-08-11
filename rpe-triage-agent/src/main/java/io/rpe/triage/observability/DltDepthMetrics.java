package io.rpe.triage.observability;

import io.rpe.triage.config.BoundaryHandler;
import io.rpe.triage.config.KafkaTriageConfig;
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
 * {@code rpe.dlt.depth{topic=...}} for the DLTs {@code rpe-triage-agent} solely writes (ADR-23).
 *
 * Per-service copy of the detection-service poller (no shared jar — microservices.md §1.4),
 * symmetric with {@code Pii} / {@code KafkaSecurity} / {@code BoundaryHandler}. Triage owns its
 * input-side DLT {@code payment.alerts.triage.DLT} (ADR-18) — NOT {@code payment.alerts.DLT}; it
 * must never poll a topic it does not own. The {@code .parked} topic holds alerts that re-poisoned
 * past the re-drive attempt cap (ADR-23); a non-empty parked topic is a page-worthy "manual
 * decision required" signal. Triage failures are advisory, so a backlog here never blocks the core
 * alert flow — but it is still surfaced, never silent.
 *
 * Depth = Σ per partition (latest − earliest offset). All admin waits are bounded (5s) so a hung
 * broker delays, never wedges, the scheduling thread.
 */
@Component
@BoundaryHandler("observability-poll-must-not-throw into the scheduling thread; a metric refresh failure logs and the next scheduled run retries")
public class DltDepthMetrics {

    private static final Logger log = LoggerFactory.getLogger(DltDepthMetrics.class);

    private static final List<String> DLT_TOPICS = List.of(
            KafkaTriageConfig.TRIAGE_DLT_TOPIC, KafkaTriageConfig.TRIAGE_DLT_TOPIC + ".parked");
    private static final long ADMIN_TIMEOUT_SEC = 5;

    // The shared, transport-authenticated Admin bean (KafkaTriageConfig#triageAdminClient) — it
    // carries the securityProps fold (kafka-security.md §4); a locally-created Admin would be
    // credential-less under SASL and silently freeze every gauge below at 0.
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
