package io.rpe.consumer;

import io.rpe.config.BoundaryHandler;
import io.rpe.config.RpeProperties;
import io.rpe.detection.DetectionResult;
import io.rpe.detection.Detector;
import io.rpe.detection.LuaGateResult;
import io.rpe.domain.AlertIntent;
import io.rpe.domain.AlertMessage;
import io.rpe.domain.PaymentEvent;
import io.rpe.observability.TraceCarrier;
import io.rpe.observability.TraceContextWriter;
import io.rpe.outbox.OutboxBatchWriter;
import io.rpe.redis.GateContractViolation;
import io.rpe.redis.RedisGate;
import io.rpe.util.Pii;
import io.rpe.util.UuidV5;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Kafka consumer for {@code payment.events}.
 *
 * Immutable constraints enforced here:
 * - Returns {@code void} — Spring Kafka silently ignores Mono/Flux return types.
 * - Rate limiter acquisition inside lane task, not on this consumer thread (ADR-03).
 * - {@code publishOn(laneScheduler)} is enforced in RedisGate — not repeated here.
 * - Kafka offset is committed after Redis gate + outbox submit; never waits for Postgres.
 *
 * <b>Error-handling model — two distinct surfaces:</b>
 * <ul>
 *   <li><b>Listener thread</b> (this class's {@code consume}): deserialization failures are
 *       thrown by the container before the listener runs; validation failures are thrown
 *       here. Both propagate to {@code DefaultErrorHandler} → DLT (not-retryable).
 *   <li><b>Lane task</b> ({@code processEvent}): exceptions can NEVER reach the error
 *       handler — the listener already returned. The lane publishes to DLT itself via
 *       {@code DeadLetterPublishingRecoverer} and acks. A failure here must never be a
 *       silent drop: every terminal path is DLT + ack, buffer (CB open), or no-ack
 *       (DLT publish itself failed → redelivery after rebalance/restart).
 * </ul>
 */
@Component
public class PaymentEventConsumer implements ConsumerSeekAware {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventConsumer.class);

    /** Bounded in-lane retries for transient gate errors; CB-open and contract violations excluded. */
    private static final Retry GATE_RETRY = Retry
            .backoff(2, Duration.ofMillis(100))
            .filter(t -> !(t instanceof CallNotPermittedException)
                      && !(t instanceof GateContractViolation));

    private final RpeProperties             props;
    private final LaneExecutorService       laneExecutorService;
    private final RateLimiterService        rateLimiterService;
    private final RedisGate                 redisGate;
    private final List<Detector>            detectors;
    private final OutboxBatchWriter         outboxBatchWriter;
    private final CbFallbackHandler         cbFallbackHandler;
    private final ObjectMapper              objectMapper;
    private final Scheduler                 jdbcScheduler;
    private final Validator                 validator;
    private final DeadLetterPublishingRecoverer dltRecoverer;
    private final MeterRegistry             meterRegistry;
    private final TraceContextWriter        traceContextWriter;

    // Partition → account_id index for partition-scoped lane drain on rebalance (ADR-09).
    // Bounded to the lane cache: PaymentEventConsumer registers pruneAccountFromIndex as the
    // lane eviction listener (see init()), so an account leaves the index the moment its lane
    // is evicted — the index can never outgrow LaneExecutorService's bounded Caffeine cache.
    private final ConcurrentHashMap<TopicPartition, Set<String>> partitionAccountIndex =
            new ConcurrentHashMap<>();

    public PaymentEventConsumer(
            RpeProperties props,
            LaneExecutorService laneExecutorService,
            RateLimiterService rateLimiterService,
            RedisGate redisGate,
            List<Detector> detectors,
            OutboxBatchWriter outboxBatchWriter,
            CbFallbackHandler cbFallbackHandler,
            ObjectMapper objectMapper,
            Scheduler jdbcScheduler,
            Validator validator,
            DeadLetterPublishingRecoverer dltRecoverer,
            MeterRegistry meterRegistry,
            TraceContextWriter traceContextWriter) {
        this.props               = props;
        this.laneExecutorService = laneExecutorService;
        this.rateLimiterService  = rateLimiterService;
        this.redisGate           = redisGate;
        this.detectors           = detectors;
        this.outboxBatchWriter   = outboxBatchWriter;
        this.cbFallbackHandler   = cbFallbackHandler;
        this.objectMapper        = objectMapper;
        this.jdbcScheduler       = jdbcScheduler;
        this.validator           = validator;
        this.dltRecoverer        = dltRecoverer;
        this.meterRegistry       = meterRegistry;
        this.traceContextWriter  = traceContextWriter;
    }

    /**
     * Bind index pruning to lane eviction so {@link #partitionAccountIndex} stays bounded by
     * the lane cache. Registered post-construction to avoid leaking {@code this} from the
     * constructor.
     */
    @PostConstruct
    void init() {
        laneExecutorService.onEviction(this::pruneAccountFromIndex);
    }

    /**
     * Removes an evicted account from the partition index. An account belongs to at most one
     * partition's set, and only the partitions assigned to this instance are present, so the
     * scan is over a handful of small sets — cheaper than maintaining a second reverse map
     * that would itself need bounding. Runs on a Caffeine maintenance thread; must not throw.
     */
    private void pruneAccountFromIndex(String accountId) {
        for (Set<String> accounts : partitionAccountIndex.values()) {
            accounts.remove(accountId);
        }
    }

    /**
     * Entry point for each payment event. Returns {@code void} — mandatory constraint.
     * Never return Mono/Flux: Spring Kafka silently ignores reactive return types.
     */
    @KafkaListener(
            id           = "payment-consumer",
            topics       = "payment.events",
            containerFactory = "paymentListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, PaymentEvent> record, Acknowledgment ack) {
        if (record.value() == null) {
            // Tombstone (null payload, no deserialization-exception header). Records that
            // FAILED deserialization are thrown by the container before the listener runs
            // and never arrive here.
            log.debug("Skipping tombstone record at offset {}", record.offset());
            ack.acknowledge();
            return;
        }

        PaymentEvent event = record.value();

        // Validate at the ingestion boundary, ON THE LISTENER THREAD — the throw must
        // propagate to DefaultErrorHandler (not-retryable → DLT). @Valid on @KafkaListener
        // params does not auto-trigger; Bean Validation requires explicit invocation.
        Set<ConstraintViolation<PaymentEvent>> violations = validator.validate(event);
        if (!violations.isEmpty()) {
            log.warn("Malformed event for account=...{}: {} constraint violation(s) — routing to DLT",
                    Pii.maskAccountId(event.accountId()), violations.size());
            throw new EventValidationException(violations.size());
        }

        TopicPartition partition = new TopicPartition(record.topic(), record.partition());

        // Maintain partition → account index for rebalance drain (ADR-09)
        partitionAccountIndex
                .computeIfAbsent(partition, k -> ConcurrentHashMap.newKeySet())
                .add(event.accountId());

        // Capture the consume-span trace context HERE, on the listener thread where it is the
        // active context (ADR-25). It cannot be read on the lane VT: the lane is a raw executor,
        // so the ThreadLocal trace context does not survive lane.submit(). Thread it explicitly.
        TraceCarrier trace = traceContextWriter.capture();
        Runnable task = () -> processEvent(record, ack, trace);

        // Single routing decision (lane vs CB buffer) lives in the handler — the previous
        // read-state-then-act switch here could interleave with the drain's NORMAL flip and
        // strand a buffered task until the next CB cycle (EADIE audit).
        cbFallbackHandler.dispatch(event.accountId(), task);
    }

    /**
     * Runs on the per-account lane virtual thread. Every terminal path either acks
     * (processed, or published to DLT), buffers (CB open), or leaves the offset
     * uncommitted (DLT publish failed) — never a silent drop.
     */
    @BoundaryHandler("kafka-listener-top-level: any processing failure must reach the DLT, never a silent drop (ADR-02)")
    private void processEvent(ConsumerRecord<String, PaymentEvent> record, Acknowledgment ack, TraceCarrier trace) {
        PaymentEvent event = record.value();
        long brokerIngestMs = record.timestamp();
        String maskedAccount = Pii.maskAccountId(event.accountId());
        MDC.put("event_id", Pii.maskEventId(event.eventId()));
        MDC.put("account_id", maskedAccount);
        try {
            // Rate limit check inside lane — never on consumer thread (ADR-03).
            // Sustained 5s breach = adversarial/misconfigured upstream → DLT per spec.
            if (!rateLimiterService.tryAcquire(event.accountId())) {
                throw new RejectedExecutionException("rate limit sustained breach");
            }

            RpeProperties.DetectionProperties det = props.detection();
            LuaGateResult result = redisGate.execute(
                    event.eventId(), event.accountId(),
                    event.amount(), event.lat(), event.lon(),
                    brokerIngestMs,
                    det.velocity().windowMs(),
                    det.velocity().velTtlMs(),
                    det.geo().geoTtlMs(),
                    det.welford().sampleCap(),
                    det.dedup().ttlSeconds(),
                    det.stats().ttlMs()
            ).retryWhen(GATE_RETRY).block();

            if (result == null) {
                // Script returned no reply — a contract violation, not a transient error
                throw new GateContractViolation(Pii.maskEventId(event.eventId()), "shape", null);
            }

            if (result.dedupBlocked()) {
                // A RE-DRIVEN record is dedup-blocked by design: post-gate failures reach the
                // DLT with the dedup key already marked (gate step 4 runs first). If the DLT
                // record carries a reconstructable outcome, re-emit the alert instead of
                // silently acking it away (EADIE audit GAP-3).
                if (handleRedrivenDuplicate(record, ack, trace)) {
                    return;
                }
                log.debug("Dedup blocked duplicate event (account=...{})", maskedAccount);
                ack.acknowledge();
                return;
            }

            DetectionResult firing = null;
            boolean alertDurable = true;
            for (Detector detector : detectors) {
                long startNanos = System.nanoTime();
                DetectionResult dr = detector.evaluate(result, event, brokerIngestMs);
                meterRegistry.timer("rpe.detection.timer",
                                "rule_type", detector.ruleName(),
                                "outcome", dr.isAlert() ? "alert" : "clean")
                        .record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
                if (dr.isAlert()) {
                    firing = dr;
                    alertDurable = submitAlert(event, dr, trace);
                    break; // first firing rule wins; alert_id is deterministic per rule
                }
            }

            if (alertDurable) {
                // Hot path done — ack after Redis + a successful outbox enqueue.
                ack.acknowledge();
            } else {
                // Outbox saturated (Postgres backpressure): the alert was NOT durably enqueued.
                // Preserve the event on the DLT — WITH the detection outcome as headers, because
                // the dedup key is already marked: a re-drive is dedup-blocked and cannot re-run
                // the detectors, so without the outcome the alert would be lost on re-drive. The
                // headers make the record self-sufficient (see handleRedrivenDuplicate).
                log.error("Alert outbox saturated for account=...{} — routing event to DLT with "
                        + "reconstructable outcome headers", maskedAccount);
                publishToDltAndAck(record,
                        new AlertNotDurableException(firing.ruleName(), firing.reason()), ack);
            }

        } catch (CallNotPermittedException e) {
            // CB just opened; hold the event for replay when the CB recovers (ADR-02).
            // Offset stays unacknowledged — with asyncAcks the gap holds the partition
            // watermark, so buffer loss on crash is genuinely not data loss.
            cbFallbackHandler.dispatch(event.accountId(), () -> processEvent(record, ack, trace));
        } catch (GateContractViolation e) {
            // Pre-reported at the RedisGate catch site (counter incremented there).
            // Deterministic per event — DLT without retry; debug only, no double emission.
            log.debug("Gate contract violation routed to DLT: {}", e.getMessage());
            publishToDltAndAck(record, e, ack);
        } catch (RejectedExecutionException e) {
            // Sustained rate-limit breach — already WARN-logged by RateLimiterService
            publishToDltAndAck(record, e, ack);
        } catch (Exception e) {
            // Transient retries exhausted (or unexpected failure). Never silently drop:
            // route to DLT so the event is preserved for replay/inspection.
            log.error("Processing failed for account=...{} after retries — routing to DLT: {}",
                    maskedAccount, e.getMessage(), e);
            publishToDltAndAck(record, e, ack);
        } finally {
            MDC.remove("event_id");
            MDC.remove("account_id");
        }
    }

    /**
     * Lane-side DLT publication. Acks ONLY if the DLT publish succeeded — if Kafka itself
     * is unavailable, the offset stays uncommitted and the event is redelivered after a
     * rebalance/restart instead of vanishing.
     */
    @BoundaryHandler("kafka-listener-DLT-publish: ack only if DLT publish succeeded; on Kafka failure leaves the offset uncommitted for redelivery")
    private void publishToDltAndAck(
            ConsumerRecord<String, PaymentEvent> record, Exception cause, Acknowledgment ack) {
        try {
            // Blocks until the send is confirmed: the recoverer is configured (and, in
            // spring-kafka 4.1.0, already defaults) to failIfSendResultIsError(true) + a bounded
            // waitForSendResultTimeout (KafkaConfig pins both). So a failed DLT publish throws here
            // rather than letting the ack below commit an offset for a record that never landed.
            dltRecoverer.accept(record, cause);
            ack.acknowledge();
        } catch (Exception dltFailure) {
            meterRegistry.counter("rpe.dlt.publish.failed").increment();
            log.error("DLT publish failed — offset left uncommitted for redelivery (partition={}, offset={})",
                    record.partition(), record.offset(), dltFailure);
        }
    }

    /** Stamped by deploy/kafka/dlt-redrive.sh on every re-driven record (ADR-23). */
    private static final String REDRIVE_ATTEMPTS_HEADER = "x-redrive-attempts";

    /**
     * Recovery path for re-driven post-gate DLT records (EADIE audit GAP-3). Such records are
     * dedup-blocked (the gate marked dedup before the original failure), so the detectors can
     * never re-run for them — the DLT record's own outcome headers are the only source of truth.
     *
     * <ul>
     *   <li>Outcome {@code ALERT_UNDURABLE} + rule header: the original firing is reconstructed
     *       deterministically — same {@code UUIDv5(eventId + ":" + rule)} alert_id — and submitted
     *       straight to the outbox. No gate re-run, no velocity/Welford/geo pollution;
     *       {@code processed_alerts ON CONFLICT} absorbs the duplicate if the original intent DID
     *       reach Postgres. If the outbox is still saturated, back to the DLT (headers re-stamped).
     *   <li>Re-driven but no reconstructable outcome (post-gate failure of unknown verdict):
     *       fail-visible — back to the DLT so the re-drive attempt cap parks it for operator
     *       decision (ADR-23), never a silent ack that pretends the record was handled.
     * </ul>
     *
     * @return {@code true} if this record was a re-drive and has been fully handled (acked or
     *         re-routed); {@code false} for an ordinary duplicate — caller acks as before.
     */
    private boolean handleRedrivenDuplicate(
            ConsumerRecord<String, PaymentEvent> record, Acknowledgment ack, TraceCarrier trace) {
        if (record.headers().lastHeader(REDRIVE_ATTEMPTS_HEADER) == null) {
            return false;   // not a re-drive — an ordinary at-least-once duplicate
        }
        PaymentEvent event = record.value();
        String outcome = headerValue(record, AlertNotDurableException.OUTCOME_HEADER);
        String rule    = headerValue(record, AlertNotDurableException.RULE_HEADER);

        if (AlertNotDurableException.OUTCOME_ALERT_UNDURABLE.equals(outcome) && rule != null) {
            String reason = headerValue(record, AlertNotDurableException.REASON_HEADER);
            DetectionResult dr = DetectionResult.alert(
                    rule, reason != null ? reason : "reconstructed-from-redrive");
            boolean durable = submitAlert(event, dr, trace);
            meterRegistry.counter("rpe.redrive.reconstructed",
                    "outcome", durable ? "submitted" : "still_saturated").increment();
            if (durable) {
                log.info("Re-driven event reconstructed alert rule={} (dedup-blocked re-run)", rule);
                ack.acknowledge();
            } else {
                publishToDltAndAck(record, new AlertNotDurableException(rule, reason), ack);
            }
            return true;
        }

        // Re-driven, dedup-blocked, no reconstructable outcome: detection cannot be repeated
        // (state already mutated; within TTL detectors are skipped, past TTL the window moved).
        // Loud + parked beats silent: the script's attempt cap routes it to <dlt>.parked.
        meterRegistry.counter("rpe.redrive.unrecoverable").increment();
        log.error("Re-driven event is dedup-blocked with no reconstructable outcome — returning "
                + "to DLT (attempt cap will park it) rather than silently acking");
        publishToDltAndAck(record, new IllegalStateException(
                "re-driven record dedup-blocked with no reconstructable outcome"), ack);
        return true;
    }

    private static String headerValue(ConsumerRecord<String, PaymentEvent> record, String name) {
        var header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * @return {@code true} if the alert intent was durably enqueued to the outbox; {@code false} if the
     *         outbox queue is saturated (Postgres backpressure) — the caller then routes the event to the
     *         DLT rather than acking a lost alert (arch-audit HIGH-1).
     */
    private boolean submitAlert(PaymentEvent event, DetectionResult dr, TraceCarrier trace) {
        UUID alertId = UuidV5.generate(
                UuidV5.RPE_NAMESPACE, event.eventId() + ":" + dr.ruleName());

        AlertMessage message = new AlertMessage(
                alertId,
                event.eventId(),
                event.accountId(),
                dr.ruleName(),
                dr.reason(),
                event.timestamp());

        // Persist the W3C trace context alongside the intent (ADR-25). The relay restores it as
        // the remote parent when it publishes to payment.alerts, so the trace is continuous
        // across the async outbox gap. NULL when no span was active — relay starts a fresh trace.
        AlertIntent intent = new AlertIntent(
                alertId,
                event.accountId(),
                objectMapper.valueToTree(message),
                Instant.now(),
                trace.traceparent(),
                trace.tracestate());

        // In-flight gap instrumentation: count the alert at the moment the hot path hands it
        // to the outbox writer. The matching confirmation counter rpe.hotpath.alerts.buffered
        // fires inside OutboxBatchWriter.submit once the intent is safely held in the volatile queue.
        meterRegistry.counter("rpe.hotpath.alerts.submitted").increment();

        // Durability-gated (was fire-and-forget): the enqueue runs on jdbcScheduler (PgJDBC pins VT
        // carriers — keep JDBC off the lane VT) and the lane VT parks on block() for the result. The
        // offer is O(1) in steady state, so the ≤50ms ADR-12 crash window is unchanged; only a saturated
        // queue blocks (bounded), and returns false so the caller preserves the event on the DLT instead
        // of acking a dropped alert. A thrown enqueue propagates to processEvent's boundary catch → DLT.
        Boolean enqueued = Mono.fromCallable(() -> outboxBatchWriter.submit(intent))
                .subscribeOn(jdbcScheduler)
                .block();
        if (Boolean.TRUE.equals(enqueued)) {
            meterRegistry.counter("rpe.outbox.submit.success").increment();
            return true;
        }
        return false;
    }

    // ── ConsumerSeekAware — partition-scoped lane drain on rebalance ──────────

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        // Drain ONLY lanes for accounts on revoked partitions — not all lanes.
        // Draining all lanes would stall active partitions (ADR-09).
        log.info("Partitions revoked: {}; draining affected lanes (5s bound)", partitions);
        List<ExecutorService> lanesToDrain         = new ArrayList<>();
        List<String>          accountsToInvalidate = new ArrayList<>();

        for (TopicPartition tp : partitions) {
            Set<String> accounts = partitionAccountIndex.remove(tp);
            if (accounts == null) continue;
            for (String accountId : accounts) {
                ExecutorService lane = laneExecutorService.getOrCreate(accountId);
                lane.shutdown();
                lanesToDrain.add(lane);
                accountsToInvalidate.add(accountId);
            }
        }

        long deadline = System.currentTimeMillis() + 5_000;
        for (ExecutorService lane : lanesToDrain) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) break;
            try {
                lane.awaitTermination(remaining, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        // Invalidate shut-down lanes from Caffeine so a fresh executor is created if the
        // partition is rebalanced back to this instance. Without invalidation, getOrCreate
        // returns the dead executor forever: every submit is rejected (the bounded-blocking
        // saturation policy throws on shut-down lanes), and from the listener that rejection
        // reaches DefaultErrorHandler → DLT after retries — loss of valid events.
        accountsToInvalidate.forEach(laneExecutorService::invalidate);
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        // No seek on assignment — continue from committed offset
    }

    @Override
    public void registerSeekCallback(ConsumerSeekCallback callback) {}
}
