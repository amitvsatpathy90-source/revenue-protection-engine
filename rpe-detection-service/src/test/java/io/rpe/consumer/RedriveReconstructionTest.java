package io.rpe.consumer;

import io.rpe.config.RpeProperties;
import io.rpe.detection.Detector;
import io.rpe.detection.LuaGateResult;
import io.rpe.domain.AlertIntent;
import io.rpe.domain.PaymentEvent;
import io.rpe.observability.TraceCarrier;
import io.rpe.outbox.OutboxBatchWriter;
import io.rpe.redis.RedisGate;
import io.rpe.util.UuidV5;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.validation.Validator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.support.Acknowledgment;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.AbstractList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * A post-gate failure (outbox saturated, detector-loop error) reaches {@code payment.events.DLT}
 * with the dedup key ALREADY marked — the gate writes dedup at step 4 before Java runs. A
 * re-drive within the dedup TTL is therefore dedup-blocked, skips the detectors, and — pre-fix —
 * acked the event without ever re-emitting the alert that was firing when the failure hit. The
 * alert was lost silently despite the DLT "preserving" the event.
 *
 * Post-fix contract: the DLT record carries the detection outcome as headers
 * ({@code x-rpe-outcome=ALERT_UNDURABLE}, {@code x-rpe-rule}, {@code x-rpe-reason}); on a
 * re-driven record ({@code x-redrive-attempts} present, stamped by dlt-redrive.sh) that is
 * dedup-blocked, the consumer reconstructs the alert intent deterministically (UUIDv5 is
 * recomputable from eventId + rule) and submits it straight to the outbox — no gate re-run, no
 * state pollution; {@code processed_alerts ON CONFLICT} absorbs any duplicate. A re-driven
 * record with NO reconstructable outcome goes back to the DLT (fail-visible; the script's
 * attempt cap parks it, ADR-23) instead of being silently acked.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class RedriveReconstructionTest {

    private static final String TOPIC = "payment.events";
    private static final String EVENT_ID = "evt-redrive-1";
    private static final String ACCOUNT = "A";

    @Mock RpeProperties props;
    @Mock LaneExecutorService laneExecutorService;
    @Mock RateLimiterService rateLimiterService;
    @Mock RedisGate redisGate;
    @Mock OutboxBatchWriter outboxBatchWriter;
    @Mock CbFallbackHandler cbFallbackHandler;
    @Mock Validator validator;
    @Mock DeadLetterPublishingRecoverer dltRecoverer;
    @Mock io.rpe.observability.TraceContextWriter traceContextWriter;

    private PaymentEventConsumer consumer;
    private Acknowledgment ack;

    @BeforeEach
    void setUp() {
        consumer = new PaymentEventConsumer(
                props, laneExecutorService, rateLimiterService, redisGate,
                List.<Detector>of(), outboxBatchWriter, cbFallbackHandler,
                new ObjectMapper().findAndRegisterModules(), Schedulers.immediate(), validator, dltRecoverer,
                new SimpleMeterRegistry(), traceContextWriter);
        ack = mock(Acknowledgment.class);

        when(validator.validate(any(PaymentEvent.class))).thenReturn(Collections.emptySet());
        when(traceContextWriter.capture()).thenReturn(TraceCarrier.EMPTY);
        when(rateLimiterService.tryAcquire(ACCOUNT)).thenReturn(true);
        when(props.detection()).thenReturn(new RpeProperties.DetectionProperties(
                new RpeProperties.VelocityProperties(60_000, 100, 120_000),
                new RpeProperties.ZScoreProperties(3.0),
                new RpeProperties.GeoProperties(900.0, 86_400_000),
                new RpeProperties.WelfordProperties(10_000),
                new RpeProperties.DedupProperties(300),
                new RpeProperties.StatsProperties(2_592_000_000L)));

        // Gate replies dedup-blocked: the re-driven event was already seen (step-0 EXISTS).
        when(redisGate.execute(anyString(), anyString(), any(), any(), any(),
                anyLong(), anyLong(), anyLong(), anyLong(), anyInt(), anyInt(), anyLong()))
                .thenReturn(Mono.just(new LuaGateResult(true, 0, 0, 0.0, 0.0, null)));

        // Route the consumer's dispatch inline so the lane task runs synchronously.
        // Post-fix API: CbFallbackHandler.dispatch(accountId, task) owns lane routing.
        doAnswer(inv -> {
            inv.<Runnable>getArgument(1).run();
            return null;
        }).when(cbFallbackHandler).dispatch(anyString(), any(Runnable.class));
        when(laneExecutorService.getOrCreate(ACCOUNT)).thenReturn(directExecutor());
    }

    private ConsumerRecord<String, PaymentEvent> record(boolean redriven, String outcome,
                                                        String rule, String reason) {
        PaymentEvent event = new PaymentEvent(
                EVENT_ID, ACCOUNT,
                new BigDecimal("10.00"), new BigDecimal("1.0"), new BigDecimal("1.0"),
                Instant.now(), "1");
        var rec = new ConsumerRecord<>(TOPIC, 0, 0L, ACCOUNT, event);
        if (redriven) {
            rec.headers().add(new RecordHeader("x-redrive-attempts",
                    "1".getBytes(StandardCharsets.UTF_8)));
        }
        if (outcome != null) {
            rec.headers().add(new RecordHeader("x-rpe-outcome",
                    outcome.getBytes(StandardCharsets.UTF_8)));
        }
        if (rule != null) {
            rec.headers().add(new RecordHeader("x-rpe-rule",
                    rule.getBytes(StandardCharsets.UTF_8)));
        }
        if (reason != null) {
            rec.headers().add(new RecordHeader("x-rpe-reason",
                    reason.getBytes(StandardCharsets.UTF_8)));
        }
        return rec;
    }

    @Test
    void redrivenAlertUndurableRecordReconstructsTheAlertDespiteDedupBlock() {
        when(outboxBatchWriter.submit(any())).thenReturn(true);

        consumer.consume(record(true, "ALERT_UNDURABLE",
                "velocity", "velocity: prior=100 >= max=100"), ack);

        ArgumentCaptor<AlertIntent> intent = ArgumentCaptor.forClass(AlertIntent.class);
        verify(outboxBatchWriter).submit(intent.capture());
        UUID expectedAlertId = UuidV5.generate(UuidV5.RPE_NAMESPACE, EVENT_ID + ":velocity");
        assertThat(intent.getValue().alertId())
                .as("reconstructed alert_id must be the SAME deterministic UUIDv5 the original "
                        + "firing produced — processed_alerts ON CONFLICT absorbs any duplicate")
                .isEqualTo(expectedAlertId);
        assertThat(intent.getValue().accountId()).isEqualTo(ACCOUNT);
        verify(ack).acknowledge();
    }

    @Test
    void redrivenRecordWithoutReconstructableOutcomeGoesBackToDltNotSilentlyAcked() {
        consumer.consume(record(true, null, null, null), ack);

        verify(outboxBatchWriter, never()).submit(any());
        // Fail-visible: back to the DLT so the re-drive attempt cap parks it (ADR-23) —
        // never a silent ack that pretends the record was handled.
        verify(dltRecoverer).accept(any(), any(Exception.class));
        verify(ack).acknowledge();
    }

    @Test
    void ordinaryDuplicateWithoutRedriveHeadersStillAcksWithoutSideEffects() {
        consumer.consume(record(false, null, null, null), ack);

        verify(outboxBatchWriter, never()).submit(any());
        verify(dltRecoverer, never()).accept(any(), any(Exception.class));
        verify(ack).acknowledge();
    }

    /** Runs submitted tasks on the calling thread so assertions can be synchronous. */
    private static ExecutorService directExecutor() {
        return new AbstractExecutorService() {
            @Override public void execute(Runnable command) { command.run(); }
            @Override public void shutdown() {}
            @Override public List<Runnable> shutdownNow() { return new AbstractList<>() {
                @Override public Runnable get(int index) { throw new IndexOutOfBoundsException(); }
                @Override public int size() { return 0; }
            }; }
            @Override public boolean isShutdown() { return false; }
            @Override public boolean isTerminated() { return false; }
            @Override public boolean awaitTermination(long timeout, TimeUnit unit) { return true; }
        };
    }
}
