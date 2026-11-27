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
import org.jspecify.annotations.NonNull;
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

import java.nio.charset.StandardCharsets;
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
 * <p>Constraints enforced here:
 * <ul>
 *   <li>Listener returns {@code void}; Spring Kafka silently ignores Mono/Flux.</li>
 *   <li>Rate limiting runs inside the lane task, never on the consumer thread (ADR-03).</li>
 *   <li>{@code publishOn(laneScheduler)} is enforced in {@link RedisGate}, not repeated here.</li>
 *   <li>Offset is committed after Redis gate + outbox enqueue; never waits for Postgres.</li>
 * </ul>
 *
 * <p>Two error-handling surfaces:
 * <ul>
 *   <li><b>Listener thread</b> ({@link #consume}): validation failures throw to
 *       {@code DefaultErrorHandler} → DLT (not-retryable).</li>
 *   <li><b>Lane task</b> ({@link #processEvent}): exceptions cannot reach the error handler.
 *       The lane publishes to the DLT itself. Every terminal path is DLT + ack, CB buffer,
 *       or no-ack (DLT publish failed → redelivery); never a silent drop.</li>
 * </ul>
 */
@Component
public class PaymentEventConsumer implements ConsumerSeekAware {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventConsumer.class);

    /**
     * Total time budget for draining revoked lanes.
     */
    private static final long DRAIN_BUDGET_MS = 5_000;

    /**
     * Stamped by deploy/kafka/dlt-redrive.sh on every re-driven record (ADR-23).
     */
    private static final String REDRIVE_ATTEMPTS_HEADER = "x-redrive-attempts";

    /**
     * Bounded in-lane retries for transient gate errors; CB-open and contract violations excluded.
     */
    private static final Retry GATE_RETRY = Retry
            .backoff(2, Duration.ofMillis(100))
            .filter(t -> !(t instanceof CallNotPermittedException)
                    && !(t instanceof GateContractViolation));

    private final RpeProperties props;
    private final LaneExecutorService laneExecutorService;
    private final RateLimiterService rateLimiterService;
    private final RedisGate redisGate;
    private final List<Detector> detectors;
    private final OutboxBatchWriter outboxBatchWriter;
    private final CbFallbackHandler cbFallbackHandler;
    private final ObjectMapper objectMapper;
    private final Scheduler jdbcScheduler;
    private final Validator validator;
    private final DeadLetterPublishingRecoverer dltRecoverer;
    private final MeterRegistry meterRegistry;
    private final TraceContextWriter traceContextWriter;

    // Partition → account index for partition-scoped drain (ADR-09); pruned on lane eviction
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
        this.props = props;
        this.laneExecutorService = laneExecutorService;
        this.rateLimiterService = rateLimiterService;
        this.redisGate = redisGate;
        this.detectors = detectors;
        this.outboxBatchWriter = outboxBatchWriter;
        this.cbFallbackHandler = cbFallbackHandler;
        this.objectMapper = objectMapper;
        this.jdbcScheduler = jdbcScheduler;
        this.validator = validator;
        this.dltRecoverer = dltRecoverer;
        this.meterRegistry = meterRegistry;
        this.traceContextWriter = traceContextWriter;
    }

    /**
     * Binds index pruning to lane eviction so the partition index stays bounded by the
     * lane cache. Registered post-construction to avoid leaking {@code this}.
     */
    @PostConstruct
    void init() {
        laneExecutorService.onEviction(this::pruneAccountFromIndex);
    }

    /**
     * Removes an evicted account from the partition index. Runs on a Caffeine maintenance
     * thread; must not throw.
     *
     * @param accountId evicted account id
     */
    private void pruneAccountFromIndex(String accountId) {
        for (Set<String> accounts : partitionAccountIndex.values()) {
            accounts.remove(accountId);
        }
    }

    /**
     * Entry point per payment event. Must return {@code void}.
     *
     * @param consumerRecord record from {@code payment.events}
     * @param ack            manual acknowledgment, committed by the lane task
     */
    @KafkaListener(
            id = "payment-consumer",
            groupId = "rpe-payment-consumer",
            topics = "payment.events",
            containerFactory = "paymentListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, PaymentEvent> consumerRecord, Acknowledgment ack) {
        if (consumerRecord.value() == null) {
            // Tombstone; failed deserialization never reaches the listener
            log.debug("Skipping tombstone record at offset {}", consumerRecord.offset());
            ack.acknowledge();
            return;
        }

        PaymentEvent event = consumerRecord.value();

        // Explicit call required: @Valid on @KafkaListener params does not trigger.
        // Throw on the listener thread so DefaultErrorHandler routes to DLT.
        Set<ConstraintViolation<PaymentEvent>> violations = validator.validate(event);
        if (!violations.isEmpty()) {
            if (log.isWarnEnabled()) { // skip eager masking when WARN is off
                log.warn("Malformed event for account=...{}: {} constraint violation(s) — routing to DLT",
                        Pii.maskAccountId(event.accountId()), violations.size());
            }
            throw new EventValidationException(violations.size());
        }

        TopicPartition partition = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
        partitionAccountIndex
                .computeIfAbsent(partition, k -> ConcurrentHashMap.newKeySet())
                .add(event.accountId());

        // Capture here: ThreadLocal trace context does not survive lane submit (ADR-25)
        TraceCarrier trace = traceContextWriter.capture();
        Runnable task = () -> processEvent(consumerRecord, ack, trace);

        // Single lane-vs-buffer routing decision lives in the handler (EADIE audit)
        cbFallbackHandler.dispatch(event.accountId(), task);
    }

    /**
     * Per-account lane task: rate limit → Redis gate → detectors → ack. Every terminal path
     * acks (processed or DLT'd), buffers (CB open), or leaves the offset uncommitted
     * (DLT publish failed).
     *
     * @param consumerRecord record being processed
     * @param ack            manual acknowledgment
     * @param trace          trace context captured on the listener thread
     */
    @BoundaryHandler("kafka-listener-top-level: any processing failure must reach the DLT, never a silent drop (ADR-02)")
    private void processEvent(ConsumerRecord<String, PaymentEvent> consumerRecord,
                              Acknowledgment ack,
                              TraceCarrier trace) {
        PaymentEvent event = consumerRecord.value();
        long brokerIngestMs = consumerRecord.timestamp();
        String maskedAccount = Pii.maskAccountId(event.accountId());
        MDC.put("event_id", Pii.maskEventId(event.eventId()));
        MDC.put("account_id", maskedAccount);
        try {
            // Inside the lane, never on the consumer thread (ADR-03)
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
                // No script reply: contract violation, not transient
                throw new GateContractViolation(Pii.maskEventId(event.eventId()), "shape", null);
            }

            if (result.dedupBlocked()) {
                ackDuplicate(consumerRecord, ack, trace, maskedAccount);
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
                    break; // first firing rule wins; alert_id deterministic per rule
                }
            }

            if (alertDurable) {
                ack.acknowledge(); // after Redis gate + successful outbox enqueue
            } else {
                // Outbox saturated and dedup key already marked: a re-drive cannot re-run
                // detectors, so the outcome travels as DLT headers (see handleRedrivenDuplicate)
                log.error("Alert outbox saturated for account=...{} — routing event to DLT with "
                        + "reconstructable outcome headers", maskedAccount);
                publishToDltAndAck(consumerRecord,
                        new AlertNotDurableException(firing.ruleName(), firing.reason()), ack);
            }

        } catch (CallNotPermittedException e) {
            // CB opened: buffer for replay; offset stays unacked, so crash is not data loss (ADR-02)
            cbFallbackHandler.dispatch(event.accountId(), () -> processEvent(consumerRecord, ack, trace));
        } catch (GateContractViolation e) {
            // Counter already incremented at the RedisGate catch site; deterministic → DLT
            log.debug("Gate contract violation routed to DLT: {}", e.getMessage());
            publishToDltAndAck(consumerRecord, e, ack);
        } catch (RejectedExecutionException e) {
            // Already WARN-logged by RateLimiterService
            publishToDltAndAck(consumerRecord, e, ack);
        } catch (Exception e) {
            // Retries exhausted or unexpected failure: preserve on DLT, never drop
            log.error("Processing failed for account=...{} after retries — routing to DLT: {}",
                    maskedAccount, e.getMessage(), e);
            publishToDltAndAck(consumerRecord, e, ack);
        } finally {
            MDC.remove("event_id");
            MDC.remove("account_id");
        }
    }

    /**
     * Acks a dedup-blocked event. Re-driven records are re-emitted from DLT headers instead.
     *
     * @param consumerRecord the dedup-blocked record
     * @param ack            manual acknowledgment
     * @param trace          trace context captured on the listener thread
     * @param maskedAccount  masked account id for logging
     */
    private void ackDuplicate(ConsumerRecord<String, PaymentEvent> consumerRecord, Acknowledgment ack,
                              TraceCarrier trace, String maskedAccount) {
        // Re-driven records are dedup-blocked by design (EADIE GAP-3)
        if (handleRedrivenDuplicate(consumerRecord, ack, trace)) {
            return;
        }
        log.debug("Dedup blocked duplicate event (account=...{})", maskedAccount);
        ack.acknowledge();
    }

    /**
     * Lane-side DLT publication. Acks only if the publish succeeded; on Kafka failure the
     * offset stays uncommitted and the event is redelivered.
     *
     * @param consumerRecord record to dead-letter
     * @param cause          failure that triggered dead-lettering
     * @param ack            manual acknowledgment
     */
    @BoundaryHandler("kafka-listener-DLT-publish: ack only if DLT publish succeeded; on Kafka failure leaves the offset uncommitted for redelivery")
    private void publishToDltAndAck(
            ConsumerRecord<String, PaymentEvent> consumerRecord, Exception cause, Acknowledgment ack) {
        try {
            // Blocks until confirmed: recoverer pinned to failIfSendResultIsError + bounded
            // waitForSendResultTimeout (KafkaConfig), so a failed publish throws before the ack
            dltRecoverer.accept(consumerRecord, cause);
            ack.acknowledge();
        } catch (Exception dltFailure) {
            meterRegistry.counter("rpe.dlt.publish.failed").increment();
            log.error("DLT publish failed — offset left uncommitted for redelivery (partition={}, offset={})",
                    consumerRecord.partition(), consumerRecord.offset(), dltFailure);
        }
    }

    /**
     * Recovers a re-driven, dedup-blocked DLT record from its outcome headers (EADIE GAP-3).
     * Detectors cannot re-run because the gate already marked dedup.
     * <ul>
     *   <li>{@code ALERT_UNDURABLE} + rule: rebuilds the alert (same UUIDv5 id) and submits it
     *       to the outbox; {@code processed_alerts ON CONFLICT} absorbs duplicates. If the
     *       outbox is still saturated, the record returns to the DLT.</li>
     *   <li>No reconstructable outcome: returns to the DLT so the attempt cap parks it for
     *       operator decision (ADR-23); never a silent ack.</li>
     * </ul>
     *
     * @param consumerRecord candidate re-driven record
     * @param ack            manual acknowledgment
     * @param trace          trace context captured on the listener thread
     * @return {@code true} if the record was a re-drive and is fully handled;
     * {@code false} for an ordinary duplicate, which the caller acks
     */
    private boolean handleRedrivenDuplicate(
            ConsumerRecord<String, PaymentEvent> consumerRecord, Acknowledgment ack, TraceCarrier trace) {
        if (consumerRecord.headers().lastHeader(REDRIVE_ATTEMPTS_HEADER) == null) {
            return false; // ordinary at-least-once duplicate
        }
        PaymentEvent event = consumerRecord.value();
        String outcome = headerValue(consumerRecord, AlertNotDurableException.OUTCOME_HEADER);
        String rule = headerValue(consumerRecord, AlertNotDurableException.RULE_HEADER);

        if (AlertNotDurableException.OUTCOME_ALERT_UNDURABLE.equals(outcome) && rule != null) {
            String reason = headerValue(consumerRecord, AlertNotDurableException.REASON_HEADER);
            DetectionResult dr = DetectionResult.alert(
                    rule, reason != null ? reason : "reconstructed-from-redrive");
            boolean durable = submitAlert(event, dr, trace);
            meterRegistry.counter("rpe.redrive.reconstructed",
                    "outcome", durable ? "submitted" : "still_saturated").increment();
            if (durable) {
                log.info("Re-driven event reconstructed alert rule={} (dedup-blocked re-run)", rule);
                ack.acknowledge();
            } else {
                publishToDltAndAck(consumerRecord, new AlertNotDurableException(rule, reason), ack);
            }
            return true;
        }

        // Detection cannot be repeated (state already mutated); fail visible, attempt cap parks it
        meterRegistry.counter("rpe.redrive.unrecoverable").increment();
        log.error("Re-driven event is dedup-blocked with no reconstructable outcome — returning "
                + "to DLT (attempt cap will park it) rather than silently acking");
        publishToDltAndAck(consumerRecord, new IllegalStateException(
                "re-driven record dedup-blocked with no reconstructable outcome"), ack);
        return true;
    }

    /**
     * @param consumerRecord record whose headers are read
     * @param name           header name
     * @return last header value as UTF-8, or {@code null} if absent
     */
    private static String headerValue(ConsumerRecord<String, PaymentEvent> consumerRecord, String name) {
        var header = consumerRecord.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    /**
     * Builds the alert intent and enqueues it to the outbox, parking the lane VT until the
     * enqueue result is known.
     *
     * @param event source payment event
     * @param dr    firing detection result
     * @param trace trace context persisted with the intent (ADR-25)
     * @return {@code true} if durably enqueued; {@code false} if the outbox is saturated
     * (Postgres backpressure) and the caller must route the event to the DLT
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

        // Trace context persisted so the relay continues the trace across the outbox gap (ADR-25)
        AlertIntent intent = new AlertIntent(
                alertId,
                event.accountId(),
                objectMapper.valueToTree(message),
                Instant.now(),
                trace.traceparent(),
                trace.tracestate());

        // Counts the hand-off; the matching buffered counter fires inside OutboxBatchWriter.submit
        meterRegistry.counter("rpe.hotpath.alerts.submitted").increment();

        // JDBC off the lane VT (PgJDBC pins carriers); lane parks on block(). Offer is O(1)
        // in steady state (ADR-12 crash window unchanged); a thrown enqueue reaches the boundary catch.
        Boolean enqueued = Mono.fromCallable(() -> outboxBatchWriter.submit(intent))
                .subscribeOn(jdbcScheduler)
                .block();
        if (Boolean.TRUE.equals(enqueued)) {
            meterRegistry.counter("rpe.outbox.submit.success").increment();
            return true;
        }
        return false;
    }

    // ── ConsumerSeekAware: partition-scoped lane drain on rebalance (ADR-09) ──

    /**
     * Drains only lanes for accounts on revoked partitions; draining all would stall active
     * partitions. Shut-down lanes are invalidated so a re-assigned partition gets a fresh executor.
     *
     * @param partitions revoked partitions
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.info("Partitions revoked: {}; draining affected lanes ({}ms bound)",
                partitions, DRAIN_BUDGET_MS);
        List<ExecutorService> lanesToDrain = new ArrayList<>();
        List<String> accountsToInvalidate = new ArrayList<>();

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

        awaitLanes(lanesToDrain, DRAIN_BUDGET_MS);

        // Else getOrCreate returns the dead executor and valid events hit DLT after retries
        accountsToInvalidate.forEach(laneExecutorService::invalidate);
    }

    /**
     * Awaits lane termination within one shared monotonic budget.
     *
     * @param lanes    shut-down lanes to await
     * @param budgetMs total wait budget in milliseconds
     */
    private static void awaitLanes(List<ExecutorService> lanes, long budgetMs) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(budgetMs);
        for (ExecutorService lane : lanes) {
            if (!awaitLane(lane, deadline)) {
                break; // budget spent, timed out, or interrupted
            }
        }
    }

    /**
     * @param lane          shut-down lane to await
     * @param deadlineNanos absolute {@link System#nanoTime()} deadline
     * @return {@code true} if the lane terminated in time; {@code false} on timeout,
     * spent budget or interrupt (flag restored)
     */
    private static boolean awaitLane(ExecutorService lane, long deadlineNanos) {
        long remaining = deadlineNanos - System.nanoTime();
        if (remaining <= 0) {
            return false;
        }
        try {
            return lane.awaitTermination(remaining, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // restore flag
            return false;
        }
    }

    @Override
    public void onPartitionsAssigned(@NonNull Map<TopicPartition, Long> assignments,
                                     @NonNull ConsumerSeekCallback callback) {
        // No-op: resume from committed offset
    }

    @Override
    public void registerSeekCallback(@NonNull ConsumerSeekCallback callback) {
        // No-op: no seeks are issued; consumption resumes from committed offsets
    }
}
