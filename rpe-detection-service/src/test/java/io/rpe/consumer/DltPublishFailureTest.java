package io.rpe.consumer;

import io.rpe.config.KafkaConfig;
import io.rpe.detection.Detector;
import io.rpe.domain.PaymentEvent;
import io.rpe.observability.TraceCarrier;
import io.rpe.observability.TraceContextWriter;
import io.rpe.outbox.OutboxBatchWriter;
import io.rpe.redis.GateContractViolation;
import io.rpe.redis.RedisGate;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.validation.Validator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.support.Acknowledgment;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Regression guards for the DLT-publish terminal path (arch-audit 2026-07-16, finding #1).
 *
 * <p><b>Correction of record:</b> the audit rated this CRITICAL on the premise that the DLT
 * recoverer was fire-and-forget BY DEFAULT (so a failed publish acked the offset anyway = silent
 * loss). That premise was WRONG for this repo's pinned spring-kafka 4.1.0:
 * {@code DeadLetterPublishingRecoverer} already defaults {@code failIfSendResultIsError} to
 * {@code true} (verified via bytecode {@code iconst_1 putfield} and empirically — the field stays
 * true with our explicit setter removed). The default was false in 2.x/3.x; Spring flipped it. So
 * there was no silent-loss-by-default bug here.
 *
 * <p>What these tests DO lock, and why they still earn their place:
 * <ol>
 *   <li><b>Config pin</b> ({@link #dltRecovererIsConfiguredToWaitForTheSendResultAndFailOnError}) —
 *       KafkaConfig sets the flag + a bounded wait EXPLICITLY, so a future dependency bump that
 *       flips the default back cannot silently reintroduce fire-and-forget. This is a version-proof
 *       pin, not proof of a fix.</li>
 *   <li><b>Consumer contract</b> ({@link #laneDltPublishFailureLeavesOffsetUncommittedAndCountsFailure})
 *       — the genuinely valuable one: when {@code accept()} throws (Kafka unavailable), the lane's
 *       terminal path leaves the offset UNACKED (Kafka redelivers) and counts the failure. That
 *       behaviour was correct already but UNTESTED; it is negative-tested (fails on a consumer that
 *       acks unconditionally).</li>
 * </ol>
 *
 * Neither test needs Docker.
 */
class DltPublishFailureTest {

    // ── Config pin: the recoverer is configured to wait-for-result + fail (version-proof) ────

    @Test
    @SuppressWarnings("unchecked")
    void dltRecovererIsConfiguredToWaitForTheSendResultAndFailOnError() throws Exception {
        // White-box on the DLPR fields. NOTE: spring-kafka 4.1.0 already DEFAULTS
        // failIfSendResultIsError to true, so this assertion holds by default too — it is a PIN
        // against a future default flip (the value was false in 2.x/3.x), not a proof that our
        // explicit setter changed behaviour. Re-proving the throw-on-failure behaviour is not ours
        // to do — it is Spring Kafka's own documented, tested contract.
        KafkaConfig config = new KafkaConfig("PLAINTEXT", "SCRAM-SHA-512", "", "", "", "");
        DeadLetterPublishingRecoverer recoverer = config.dltRecoverer(
                mock(KafkaTemplate.class), mock(KafkaTemplate.class));

        assertThat(readBoolean(recoverer, "failIfSendResultIsError"))
                .as("the recoverer must fail (not fire-and-forget) on a failed DLT send, so the "
                        + "caller never acks a record that did not reach the DLT — pinned so a future "
                        + "spring-kafka default flip cannot silently reintroduce fire-and-forget")
                .isTrue();

        assertThat(readDuration(recoverer, "waitForSendResultTimeout"))
                .as("the result-wait must be bounded — a wedged broker must not hang the lane VT "
                        + "inside accept() indefinitely")
                .isPositive()
                .isLessThanOrEqualTo(Duration.ofSeconds(30));
    }

    private static boolean readBoolean(Object target, String field) throws Exception {
        Field f = DeadLetterPublishingRecoverer.class.getDeclaredField(field);
        f.setAccessible(true);
        return f.getBoolean(target);
    }

    private static Duration readDuration(Object target, String field) throws Exception {
        Field f = DeadLetterPublishingRecoverer.class.getDeclaredField(field);
        f.setAccessible(true);
        return (Duration) f.get(target);
    }

    // ── Mechanism 2: a throwing recoverer must leave the offset UNACKED + count the failure ──

    @Test
    void laneDltPublishFailureLeavesOffsetUncommittedAndCountsFailure() {
        SimpleMeterRegistry reg = new SimpleMeterRegistry();

        RateLimiterService rateLimiter = mock(RateLimiterService.class);
        when(rateLimiter.tryAcquire(anyString())).thenReturn(true);

        // The gate deterministically fails with a per-event contract violation (not retried,
        // routed straight to the lane's DLT path — the terminal path finding #1 is about).
        RedisGate gate = mock(RedisGate.class);
        when(gate.execute(anyString(), anyString(), any(), any(), any(),
                anyLong(), anyLong(), anyLong(), anyLong(), anyInt(), anyInt(), anyLong()))
                .thenReturn(Mono.error(new GateContractViolation("evt", "shape", null)));

        CbFallbackHandler cb = mock(CbFallbackHandler.class);
        // NORMAL routing: run the lane task inline on the calling thread so processEvent executes.
        doAnswer(inv -> { ((Runnable) inv.getArgument(1)).run(); return null; })
                .when(cb).dispatch(anyString(), any(Runnable.class));

        Validator validator = mock(Validator.class);
        when(validator.validate(any())).thenReturn(Collections.emptySet());

        // The injected failure: the DLT publish itself fails (Kafka unavailable). With finding #1's
        // fix this is exactly what a failed send now looks like to the consumer (mechanism 1).
        DeadLetterPublishingRecoverer recoverer = mock(DeadLetterPublishingRecoverer.class);
        doThrow(new RuntimeException("broker unreachable")).when(recoverer).accept(any(), any());

        TraceContextWriter traceWriter = mock(TraceContextWriter.class);
        when(traceWriter.capture()).thenReturn(TraceCarrier.EMPTY);

        PaymentEventConsumer consumer = new PaymentEventConsumer(
                realProps(), mock(LaneExecutorService.class), rateLimiter, gate,
                List.<Detector>of(), mock(OutboxBatchWriter.class), cb,
                new ObjectMapper(), Schedulers.immediate(), validator,
                recoverer, reg, traceWriter);

        Acknowledgment ack = mock(Acknowledgment.class);
        consumer.consume(record("acct-1"), ack);

        verify(recoverer).accept(any(), any());
        // The offset must NOT be acked when the DLT publish failed — it stays uncommitted so
        // asyncAcks holds the watermark and Kafka redelivers, rather than committing past a lost record.
        verify(ack, never()).acknowledge();
        assertThat(reg.counter("rpe.dlt.publish.failed").count())
                .as("a failed DLT publish must be counted, not silent")
                .isEqualTo(1.0);
    }

    // ── fixtures ─────────────────────────────────────────────────────────────────────────────

    private static PaymentEvent samplePaymentEvent(String accountId) {
        return new PaymentEvent("evt-" + accountId, accountId,
                new BigDecimal("10.00"), new BigDecimal("1.0"), new BigDecimal("1.0"),
                Instant.now(), "1");
    }

    private static ConsumerRecord<String, PaymentEvent> record(String accountId) {
        return new ConsumerRecord<>("payment.events", 0, 0L, accountId, samplePaymentEvent(accountId));
    }

    /** Real properties so the gate-arg accessors (props.detection().velocity()...) resolve. */
    private static io.rpe.config.RpeProperties realProps() {
        return new io.rpe.config.RpeProperties(
                new io.rpe.config.RpeProperties.ResilienceProperties(
                        new io.rpe.config.RpeProperties.RedisResilienceProperties(500)),
                new io.rpe.config.RpeProperties.RateLimitingProperties(500, 2000, 60_000, 5_000),
                new io.rpe.config.RpeProperties.DetectionProperties(
                        new io.rpe.config.RpeProperties.VelocityProperties(60_000, 100, 120_000),
                        new io.rpe.config.RpeProperties.ZScoreProperties(3.0),
                        new io.rpe.config.RpeProperties.GeoProperties(900.0, 86_400_000),
                        new io.rpe.config.RpeProperties.WelfordProperties(10_000),
                        new io.rpe.config.RpeProperties.DedupProperties(300),
                        new io.rpe.config.RpeProperties.StatsProperties(2_592_000_000L)));
    }
}
