package io.rpe.consumer;

import io.rpe.config.RpeProperties;
import io.rpe.detection.Detector;
import io.rpe.domain.PaymentEvent;
import io.rpe.outbox.OutboxBatchWriter;
import io.rpe.redis.RedisGate;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.validation.Validator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.support.Acknowledgment;
import reactor.core.scheduler.Scheduler;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Lifecycle of the consumer's {@code partition → account_id} index.
 *
 * The index is the source of truth for ADR-09 partition-scoped lane drain, and is bounded to
 * the (bounded) Caffeine lane cache by pruning an account the moment its lane is evicted. These
 * tests drive the real {@code consume} / {@code onPartitionsRevoked} logic with mocked
 * collaborators and assert the three index behaviours:
 *
 *   1. an account is indexed under its partition when an event is consumed;
 *   2. an account is pruned from the index when its lane is evicted (the unbounded-growth fix);
 *   3. a revoke drains ONLY the revoked partition's accounts — never lanes on still-assigned
 *      partitions (ADR-09: draining all lanes would stall active partitions).
 */
@ExtendWith(MockitoExtension.class)
class PaymentEventConsumerIndexTest {

    private static final String TOPIC = "payment.events";

    @Mock RpeProperties props;
    @Mock LaneExecutorService laneExecutorService;
    @Mock RateLimiterService rateLimiterService;
    @Mock RedisGate redisGate;
    @Mock OutboxBatchWriter outboxBatchWriter;
    @Mock CbFallbackHandler cbFallbackHandler;
    @Mock ObjectMapper objectMapper;
    @Mock Scheduler jdbcScheduler;
    @Mock Validator validator;
    @Mock DeadLetterPublishingRecoverer dltRecoverer;
    @Mock io.rpe.observability.TraceContextWriter traceContextWriter;

    private PaymentEventConsumer newConsumer() {
        return new PaymentEventConsumer(
                props, laneExecutorService, rateLimiterService, redisGate,
                List.<Detector>of(), outboxBatchWriter, cbFallbackHandler,
                objectMapper, jdbcScheduler, validator, dltRecoverer,
                new SimpleMeterRegistry(), traceContextWriter);
    }

    /** A record on {@code partition} keyed by {@code accountId}; passes validation (mocked). */
    private ConsumerRecord<String, PaymentEvent> record(int partition, String accountId) {
        PaymentEvent event = new PaymentEvent(
                "evt-" + accountId, accountId,
                new BigDecimal("10.00"), new BigDecimal("1.0"), new BigDecimal("1.0"),
                Instant.now(), "1");
        return new ConsumerRecord<>(TOPIC, partition, 0L, accountId, event);
    }

    @Test
    void revokeDrainsOnlyTheRevokedPartitionsAccounts() {
        PaymentEventConsumer consumer = newConsumer();
        Acknowledgment ack = mock(Acknowledgment.class);

        when(validator.validate(any(PaymentEvent.class))).thenReturn(Collections.emptySet());

        // Distinct lane per account so we can assert exactly which lanes get drained.
        // (Lane routing on consume now lives in CbFallbackHandler.dispatch — mocked here —
        // so getOrCreate is only invoked by the revoke drain itself.)
        ExecutorService laneA = mock(ExecutorService.class);
        ExecutorService laneB = mock(ExecutorService.class);
        when(laneExecutorService.getOrCreate("A")).thenReturn(laneA);
        when(laneExecutorService.getOrCreate("B")).thenReturn(laneB);

        // A and B land on partition 0; C on partition 1 — each indexed on consume.
        consumer.consume(record(0, "A"), ack);
        consumer.consume(record(0, "B"), ack);
        consumer.consume(record(1, "C"), ack);

        consumer.onPartitionsRevoked(List.of(new TopicPartition(TOPIC, 0)));

        // Partition-0 lanes drained + invalidated; partition-1 (C) left untouched.
        verify(laneA).shutdown();
        verify(laneB).shutdown();
        verify(laneExecutorService, never()).getOrCreate("C");
        verify(laneExecutorService).invalidate("A");
        verify(laneExecutorService).invalidate("B");
        verify(laneExecutorService, never()).invalidate("C");
    }

    @Test
    void laneEvictionPrunesAccountSoRevokeDoesNotDrainIt() {
        PaymentEventConsumer consumer = newConsumer();
        Acknowledgment ack = mock(Acknowledgment.class);

        when(validator.validate(any(PaymentEvent.class))).thenReturn(Collections.emptySet());

        ExecutorService laneB = mock(ExecutorService.class);
        when(laneExecutorService.getOrCreate("B")).thenReturn(laneB);

        // Wire the eviction → prune callback (normally invoked via @PostConstruct).
        consumer.init();
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Consumer<String>> captor = ArgumentCaptor.forClass(Consumer.class);
        verify(laneExecutorService).onEviction(captor.capture());
        Consumer<String> evictionListener = captor.getValue();

        consumer.consume(record(0, "A"), ack);
        consumer.consume(record(0, "B"), ack);

        // A's lane is evicted from the bounded Caffeine cache → index must drop A.
        evictionListener.accept("A");

        consumer.onPartitionsRevoked(List.of(new TopicPartition(TOPIC, 0)));

        // A was pruned — no longer in the index, so it is not drained on revoke. B still is.
        verify(laneExecutorService, never()).getOrCreate("A");
        verify(laneB).shutdown();
        verify(laneExecutorService, never()).invalidate("A");
        verify(laneExecutorService).invalidate("B");
    }
}
