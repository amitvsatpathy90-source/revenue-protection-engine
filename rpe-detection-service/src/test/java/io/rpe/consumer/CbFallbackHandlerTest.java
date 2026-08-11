package io.rpe.consumer;

import io.rpe.config.RpeProperties;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Drives the real Redis CircuitBreaker through its transitions to lock down the C+D
 * fallback state machine (ADR-02): NORMAL → BUFFERING → PAUSED → DRAINING → NORMAL,
 * the pause/resume pairing, and the two race-hardening invariants from the EADIE audit:
 *
 *   1. a close event mid-drain must NOT flip NORMAL while the buffer is non-empty
 *      (the old CAS flipped early — new events overtook older buffered events for the
 *      same account, inverting per-account state-mutation order);
 *   2. a task dispatched around the drain-completion window must never strand in the
 *      buffer (the old listener-side read-state-then-act could offer into a buffer
 *      nothing would ever drain again).
 *
 * NOTE: these tests force CB transitions manually. In production the OPEN→HALF_OPEN
 * transition fires only because application.yml sets
 * automaticTransitionFromOpenToHalfOpenEnabled — the fallback stops all traffic to the
 * open breaker, so the lazy default would never transition. That wiring is pinned
 * separately by RedisCircuitBreakerAutoTransitionTest.
 *
 * INSPECTION-ONLY (finding #2, arch-audit 2026-07-16): the invariant "dispatch never holds
 * transitionLock across a lane submit" (the fix moved the raced-NORMAL submit OUTSIDE the lock)
 * has no automated test. Reaching that branch deterministically needs the state to flip to NORMAL
 * BETWEEN dispatch's two reads of the private volatile field — not forcible from a test without a
 * production-only seam whose surface area is not worth it. The routing correctness of that path is
 * covered by {@link #dispatchAcrossDrainCompletionIsNeverStranded}; the lock-hold itself is guarded
 * by review + the assertion in dispatch's own comment.
 */
class CbFallbackHandlerTest {

    private CircuitBreaker cb;
    private CbFallbackHandler handler;
    private MessageListenerContainer container;
    private LaneExecutorService lanes;

    /** Runnables executed through a lane, in execution order. */
    private final ConcurrentLinkedQueue<Runnable> laneExecutions = new ConcurrentLinkedQueue<>();

    @BeforeEach
    void setUp() {
        CircuitBreakerRegistry registry = CircuitBreakerRegistry.ofDefaults();
        cb = registry.circuitBreaker("redis");

        lanes = mock(LaneExecutorService.class);

        container = mock(MessageListenerContainer.class);
        when(container.getListenerId()).thenReturn("payment-consumer");
        KafkaListenerEndpointRegistry listenerRegistry = mock(KafkaListenerEndpointRegistry.class);
        when(listenerRegistry.getListenerContainers()).then(inv -> List.of(container));

        RpeProperties props = new RpeProperties(
                new RpeProperties.ResilienceProperties(
                        new RpeProperties.RedisResilienceProperties(2)), // buffer size = 2
                null, null);

        handler = new CbFallbackHandler(props, lanes, listenerRegistry, registry, new SimpleMeterRegistry());
    }

    /** Lane whose submit executes immediately on the caller and records the execution. */
    private ExecutorService recordingLane() {
        ExecutorService lane = mock(ExecutorService.class);
        doAnswer(inv -> {
            Runnable r = inv.getArgument(0);
            laneExecutions.add(r);
            r.run();
            return null;
        }).when(lane).submit(any(Runnable.class));
        return lane;
    }

    @Test
    void startsInNormal() {
        assertThat(handler.currentState()).isEqualTo(CbFallbackHandler.State.NORMAL);
    }

    @Test
    void cbOpenTransitionsToBuffering() {
        cb.transitionToOpenState();
        assertThat(handler.currentState()).isEqualTo(CbFallbackHandler.State.BUFFERING);
    }

    @Test
    void dispatchInNormalGoesStraightToTheLane() {
        ExecutorService lane = recordingLane();
        when(lanes.getOrCreate("a1")).thenReturn(lane);
        AtomicBoolean ran = new AtomicBoolean();

        handler.dispatch("a1", () -> ran.set(true));

        assertThat(ran).isTrue();
        assertThat(handler.currentState()).isEqualTo(CbFallbackHandler.State.NORMAL);
    }

    @Test
    void bufferFullTransitionsToPausedAndPausesConsumer() {
        cb.transitionToOpenState(); // → BUFFERING

        handler.dispatch("a1", () -> {});
        handler.dispatch("a2", () -> {});
        assertThat(handler.currentState()).isEqualTo(CbFallbackHandler.State.BUFFERING);

        handler.dispatch("a3", () -> {}); // buffer full → PAUSED

        assertThat(handler.currentState()).isEqualTo(CbFallbackHandler.State.PAUSED);
        verify(container, atLeastOnce()).pause();
        verifyNoInteractions(lanes); // nothing may reach a lane while the breaker is open
    }

    @Test
    void halfOpenThenClosedDrainsAndResumesToNormal() {
        ExecutorService lane = recordingLane();
        when(lanes.getOrCreate(anyString())).thenReturn(lane);
        AtomicBoolean drained = new AtomicBoolean();

        cb.transitionToOpenState();               // → BUFFERING
        handler.dispatch("a1", () -> drained.set(true));
        cb.transitionToHalfOpenState();           // OPEN_TO_HALF_OPEN → DRAINING (probe trickle)
        cb.transitionToClosedState();             // HALF_OPEN_TO_CLOSED → drain floods + flips

        Awaitility.await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
            assertThat(handler.currentState()).isEqualTo(CbFallbackHandler.State.NORMAL);
            assertThat(drained).isTrue();
        });
        verify(container, atLeastOnce()).resume();
    }

    /**
     * Reopen from a clean BUFFERING state (not mid-drain) must never resume the consumer —
     * resume is paired only with the drain-owned DRAINING→NORMAL flip.
     */
    @Test
    void reopenFromBufferingNeverResumes() {
        cb.transitionToOpenState(); // CLOSED_TO_OPEN → BUFFERING
        assertThat(handler.currentState()).isEqualTo(CbFallbackHandler.State.BUFFERING);

        cb.transitionToHalfOpenState(); // OPEN_TO_HALF_OPEN → DRAINING (empty buffer)
        cb.transitionToOpenState();     // HALF_OPEN_TO_OPEN → back to BUFFERING

        Awaitility.await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                assertThat(handler.currentState()).isEqualTo(CbFallbackHandler.State.BUFFERING));
        verify(container, never()).resume();
    }

    /**
     * EADIE invariant 1: HALF_OPEN_TO_CLOSED arriving while the drain is mid-flight (buffer
     * still non-empty) must NOT flip NORMAL — the drain thread is the sole owner of that
     * transition and flips only once the buffer is observed empty under the lock. The old
     * implementation CAS'd DRAINING→NORMAL directly from the close event and resumed the
     * consumer over a non-empty buffer.
     */
    @Test
    void closeEventMidDrainDoesNotFlipNormalWhileBufferNonEmpty() throws Exception {
        CountDownLatch laneBlocked  = new CountDownLatch(1);
        CountDownLatch releaseLane  = new CountDownLatch(1);

        ExecutorService blockingLane = mock(ExecutorService.class);
        doAnswer(inv -> {
            laneBlocked.countDown();
            assertThat(releaseLane.await(5, TimeUnit.SECONDS)).isTrue();
            Runnable r = inv.getArgument(0);
            laneExecutions.add(r);
            return null;
        }).when(blockingLane).submit(any(Runnable.class));
        when(lanes.getOrCreate(anyString())).thenReturn(blockingLane);

        cb.transitionToOpenState();               // → BUFFERING
        handler.dispatch("a1", () -> {});
        handler.dispatch("a2", () -> {});
        cb.transitionToHalfOpenState();           // → DRAINING; drain blocks in lane submit of a1

        assertThat(laneBlocked.await(5, TimeUnit.SECONDS)).isTrue();
        cb.transitionToClosedState();             // close lands MID-DRAIN, buffer still holds a2

        // The old code flipped NORMAL + resumed right here. The fix must hold DRAINING.
        Thread.sleep(300);
        assertThat(handler.currentState())
                .as("close mid-drain must not flip NORMAL while buffered tasks remain")
                .isEqualTo(CbFallbackHandler.State.DRAINING);
        verify(container, never()).resume();

        releaseLane.countDown();                  // let the drain finish a1, then a2

        Awaitility.await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
            assertThat(handler.currentState()).isEqualTo(CbFallbackHandler.State.NORMAL);
            assertThat(laneExecutions).hasSize(2);
        });
        verify(container, atLeastOnce()).resume();
    }

    /**
     * EADIE invariant 2: a task dispatched while the drain completes is either drained or
     * takes the lane path after the flip — never stranded in a buffer nothing will empty.
     * (The old listener-side read-state-then-act switch could offer into the buffer after
     * the drain's final poll; the task then waited for a next CB cycle that may never come.)
     */
    @Test
    void dispatchAcrossDrainCompletionIsNeverStranded() {
        ExecutorService lane = recordingLane();
        when(lanes.getOrCreate(anyString())).thenReturn(lane);
        AtomicBoolean before = new AtomicBoolean();
        AtomicBoolean after  = new AtomicBoolean();

        cb.transitionToOpenState();
        handler.dispatch("a1", () -> before.set(true));   // buffered
        cb.transitionToHalfOpenState();
        cb.transitionToClosedState();

        Awaitility.await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                assertThat(handler.currentState()).isEqualTo(CbFallbackHandler.State.NORMAL));

        handler.dispatch("a2", () -> after.set(true));    // post-flip → straight to lane

        Awaitility.await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
            assertThat(before).as("buffered task must be drained, not stranded").isTrue();
            assertThat(after).as("post-drain dispatch must take the lane path").isTrue();
        });
    }
}
