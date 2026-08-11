package io.rpe.consumer;

import io.rpe.config.RpeProperties;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Component;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

/**
 * C+D hybrid Redis circuit-breaker fallback (ADR-02).
 *
 * DLT is explicitly NOT used for Redis unavailability — payment events are unique and
 * unrepeatable. Sending to DLT permanently removes the event from the detection pipeline.
 *
 * State machine (all transitions single-owned, under one lock):
 *   NORMAL    → [CB opens]      → BUFFERING
 *   BUFFERING → [buffer full]   → PAUSED
 *   PAUSED    → [CB half-open]  → DRAINING → NORMAL
 *   BUFFERING → [CB half-open]  → DRAINING → NORMAL
 *
 * Buffered tasks have uncommitted Kafka offsets. With {@code asyncAcks=true} on the payment
 * container (KafkaConfig), an uncommitted offset HOLDS the partition's commit watermark —
 * later completions cannot commit past a buffered event — so "buffer loss on crash is not
 * data loss" is genuinely true: Kafka redelivers from the gap on restart.
 *
 * <b>Race-hardening (EADIE audit):</b> the previous implementation choreographed transitions
 * with lone CAS operations across three threads (listener, CB event publisher, drain VT) and
 * had two proven races: (1) a task offered in the window between the drain's final empty poll
 * and the DRAINING→NORMAL flip was stranded until the NEXT CB cycle — potentially forever;
 * (2) {@code HALF_OPEN_TO_CLOSED} flipped NORMAL mid-drain with a non-empty buffer, letting
 * new events overtake older buffered events for the same account (per-account state-mutation
 * order inverted; stale geo re-applied after newer state). Now:
 * <ul>
 *   <li>{@link #dispatch} is the single routing decision (lane vs buffer). The NORMAL fast
 *       path is a lock-free volatile read; every other decision re-checks state UNDER the
 *       transition lock, so an offer can never interleave with the NORMAL flip.
 *   <li>The DRAINING→NORMAL flip has exactly one owner — the drain thread — and fires only
 *       when the buffer is observed empty AND the breaker is CLOSED, both under the lock.
 *       A close event mid-drain no longer short-circuits the drain.
 *   <li>While the breaker is HALF_OPEN the drain trickles at most the breaker's permitted
 *       probe count per pacing interval instead of flooding: flooding bounced
 *       (buffer − permits) tasks straight back through CallNotPermittedException → dispatch
 *       → buffer, a churn storm during every recovery.
 * </ul>
 *
 * The consumer is paused ONLY on the BUFFERING → PAUSED transition and resumed ONLY on the
 * DRAINING → NORMAL flip; resume on an un-paused container is a harmless no-op, so the
 * pairing stays balanced across every path.
 *
 * NOTE: this component functions only with the {@code redis} breaker's
 * {@code automaticTransitionFromOpenToHalfOpenEnabled: true} (application.yml). Once the
 * breaker opens, this fallback stops ALL traffic to it, and R4j's OPEN→HALF_OPEN transition
 * is lazy by default — without the automatic transition the breaker and the buffer wait on
 * each other forever (see RedisCircuitBreakerAutoTransitionTest).
 */
@Component
public class CbFallbackHandler {

    private static final Logger log = LoggerFactory.getLogger(CbFallbackHandler.class);

    /** Pacing interval for the drain while the breaker's verdict is pending (half-open)
     *  or the buffer momentarily looks empty. VT sleep — no carrier pin. */
    private static final long DRAIN_PACE_MS = 50;

    public enum State { NORMAL, BUFFERING, PAUSED, DRAINING }

    private record BufferedTask(String accountId, Runnable task) {}

    private volatile State state = State.NORMAL;

    /** Guards every state transition and every non-NORMAL routing decision. Never held
     *  around blocking work — only O(1) queue ops and container pause/resume flags. */
    private final ReentrantLock transitionLock = new ReentrantLock();

    /** True while a drain thread exists. Written only under {@link #transitionLock}. */
    private boolean drainActive = false;

    private final ArrayBlockingQueue<BufferedTask> buffer;
    private final LaneExecutorService              laneExecutorService;
    private final KafkaListenerEndpointRegistry    listenerRegistry;
    private final CircuitBreaker                   circuitBreaker;
    private final int                              probeBurst;

    public CbFallbackHandler(
            RpeProperties props,
            LaneExecutorService laneExecutorService,
            KafkaListenerEndpointRegistry listenerRegistry,
            CircuitBreakerRegistry cbRegistry,
            MeterRegistry meterRegistry) {
        this.buffer              = new ArrayBlockingQueue<>(props.resilience().redis().bufferSize());
        this.laneExecutorService = laneExecutorService;
        this.listenerRegistry    = listenerRegistry;
        this.circuitBreaker      = cbRegistry.circuitBreaker("redis");
        this.probeBurst          = circuitBreaker.getCircuitBreakerConfig()
                .getPermittedNumberOfCallsInHalfOpenState();

        // Buffer depth is the early-warning signal that PAUSED is approaching
        Gauge.builder("rpe.cb.buffer.size", buffer, ArrayBlockingQueue::size)
                .description("Events held in the CB fallback buffer (uncommitted offsets)")
                .register(meterRegistry);

        circuitBreaker.getEventPublisher()
                .onStateTransition(event -> {
                    switch (event.getStateTransition()) {
                        case CLOSED_TO_OPEN, HALF_OPEN_TO_OPEN -> toBuffering();
                        case OPEN_TO_HALF_OPEN                 -> toDraining();
                        case HALF_OPEN_TO_CLOSED               -> onCbClosed();
                        default -> { /* ignored */ }
                    }
                });
    }

    public State currentState() { return state; }

    /**
     * The single routing decision for a lane task: submit to the account's lane (NORMAL) or
     * hold it in the CB buffer (breaker open/recovering). Called by the listener for fresh
     * events and by the lane for CallNotPermittedException retries.
     *
     * The NORMAL fast path is a lock-free volatile read. A read that races a CLOSED_TO_OPEN
     * transition self-corrects: the submitted task fails with CallNotPermittedException in
     * the gate and re-enters here, by which point the state is BUFFERING.
     *
     * On buffer-full the consumer is paused via the BUFFERING → PAUSED transition. An event
     * that could not be buffered is safe: its offset stays unacknowledged, and asyncAcks
     * holds the partition watermark at the gap — Kafka redelivers it.
     */
    public void dispatch(String accountId, Runnable task) {
        if (state == State.NORMAL) {
            laneExecutorService.getOrCreate(accountId).submit(task);
            return;
        }
        boolean toLane;
        transitionLock.lock();
        try {
            toLane = (state == State.NORMAL);   // raced the NORMAL flip — take the lane path
            if (!toLane) {
                if (buffer.offer(new BufferedTask(accountId, task))) {
                    return;
                }
                if (state == State.BUFFERING) {
                    state = State.PAUSED;
                    pauseConsumer();
                    log.warn("CB buffer full ({} events): transitioning to PAUSED, consumer paused",
                            buffer.size());
                } else {
                    log.warn("CB buffer full in state {} — event held via uncommitted offset (asyncAcks "
                            + "gap holds the watermark; Kafka redelivers)", state);
                }
                return;
            }
        } finally {
            transitionLock.unlock();
        }
        // Submit AFTER releasing the lock. INSPECTION-GUARDED, no automated test (finding #2): a
        // regression here (moving this back inside the try/finally) would only deadlock under the
        // hard-to-force raced-NORMAL + full-lane-queue + concurrent-transition combination — keep it
        // outside the lock. LaneExecutorService.BoundedBlockingSubmitPolicy parks the
        // submitter with NO deadline when the lane queue is full, and this branch fires precisely when
        // the drain has just flooded up to bufferSize tasks into 200-deep lanes — so queue-full here is
        // the expected case, not an edge. Holding transitionLock across that park blocks the R4j
        // transition handlers (toBuffering/toDraining) and the drain's terminal NORMAL flip: the CB
        // state machine waits on a lane the lane cannot drain, which is the permanent wedge ADR-26
        // exists to prevent. Safety is unchanged — this is the same shape as the lock-free fast path
        // above: if the breaker reopens between the unlock and the submit, the task fails with
        // CallNotPermittedException in the gate and re-enters dispatch, by which point state is
        // BUFFERING. The routing DECISION still happens under the lock, so an offer still cannot
        // interleave with the NORMAL flip.
        laneExecutorService.getOrCreate(accountId).submit(task);
    }

    private void toBuffering() {
        transitionLock.lock();
        try {
            if (state == State.NORMAL || state == State.DRAINING) {
                state = State.BUFFERING;
                log.warn("Redis CB opened: transitioning to BUFFERING");
            }
            // PAUSED stays PAUSED: the consumer is already paused and the buffer already
            // full; the next OPEN_TO_HALF_OPEN drains and resumes.
        } finally {
            transitionLock.unlock();
        }
    }

    private void toDraining() {
        transitionLock.lock();
        try {
            if (state == State.BUFFERING || state == State.PAUSED) {
                log.info("Redis CB half-open: draining {} buffered tasks", buffer.size());
                state = State.DRAINING;
                if (!drainActive) {
                    drainActive = true;
                    startDrainThread();
                }
            }
        } finally {
            transitionLock.unlock();
        }
    }

    /**
     * Defensive completion: normally the drain thread itself observes CLOSED and flips
     * NORMAL. This handles the edge where the drain exited (state briefly left DRAINING)
     * before the close event landed — without it, a DRAINING state with no drain thread
     * would strand the buffer.
     */
    private void onCbClosed() {
        transitionLock.lock();
        try {
            if (state == State.DRAINING && !drainActive) {
                drainActive = true;
                startDrainThread();
            }
        } finally {
            transitionLock.unlock();
        }
    }

    private void startDrainThread() {
        Thread.ofVirtual().name("rpe-cb-drain").start(this::drainLoop);
    }

    /**
     * Sole owner of the DRAINING → NORMAL transition. Re-submits buffered tasks through the
     * normal lane path (per-account ordering preserved — each task re-enters its account's
     * single-threaded lane, and new arrivals keep buffering behind them until the flip).
     *
     * Pacing: while the breaker is HALF_OPEN, only its permitted probe count is submitted
     * per interval — the probes decide the breaker's fate; flooding would bounce the rest
     * straight back into the buffer. Once CLOSED, the remainder floods. If the breaker
     * reopens mid-drain, HALF_OPEN_TO_OPEN flips the state to BUFFERING and this thread
     * exits; in-flight tasks re-buffer via the CallNotPermittedException → dispatch path.
     */
    private void drainLoop() {
        boolean interrupted = false;
        try {
            while (true) {
                boolean cbClosed = circuitBreaker.getState() == CircuitBreaker.State.CLOSED;
                int burst = cbClosed ? Integer.MAX_VALUE : probeBurst;
                int submitted = 0;
                BufferedTask task;
                while (submitted < burst && (task = buffer.poll()) != null) {
                    laneExecutorService.getOrCreate(task.accountId()).submit(task.task());
                    submitted++;
                }

                if (!cbClosed) {
                    transitionLock.lock();
                    try {
                        if (state != State.DRAINING) {
                            return;   // breaker reopened — tasks stay buffered for the next cycle
                        }
                    } finally {
                        transitionLock.unlock();
                    }
                    if (!pace()) { interrupted = true; return; }
                    continue;
                }

                if (submitted > 0) {
                    continue;   // CLOSED and making progress — keep flooding
                }

                // CLOSED and the buffer looked empty: attempt the terminal transition. The
                // empty re-check happens UNDER the lock, so a dispatch that buffered in the
                // meantime is either seen here (continue draining) or took the lane path
                // after the flip — stranding is impossible by construction.
                transitionLock.lock();
                try {
                    if (state != State.DRAINING) {
                        return;
                    }
                    if (!buffer.isEmpty()) {
                        continue;
                    }
                    state = State.NORMAL;
                    resumeConsumer();
                    log.info("Drain complete: Redis CB closed, buffer empty — back to NORMAL, consumer resumed");
                    return;
                } finally {
                    transitionLock.unlock();
                }
            }
        } finally {
            transitionLock.lock();
            try {
                drainActive = false;
                // Respawn guard: if OPEN→HALF_OPEN re-flipped the state to DRAINING while this
                // thread was exiting, a drain must exist or the buffer strands. Skipped on
                // interrupt (shutdown) — at that point redelivery-on-restart is the safety net.
                if (!interrupted && state == State.DRAINING) {
                    drainActive = true;
                    startDrainThread();
                }
            } finally {
                transitionLock.unlock();
            }
        }
    }

    /** @return false when interrupted (shutdown) — caller exits without respawn. */
    private boolean pace() {
        try {
            Thread.sleep(DRAIN_PACE_MS);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private void pauseConsumer() {
        listenerRegistry.getListenerContainers()
                .stream()
                .filter(c -> "payment-consumer".equals(c.getListenerId()))
                .forEach(c -> c.pause());
    }

    private void resumeConsumer() {
        listenerRegistry.getListenerContainers()
                .stream()
                .filter(c -> "payment-consumer".equals(c.getListenerId()))
                .forEach(c -> c.resume());
    }
}
