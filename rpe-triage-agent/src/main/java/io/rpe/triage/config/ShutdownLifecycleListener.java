package io.rpe.triage.config;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.ReadinessState;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Marks the start of a graceful shutdown (ADR-22). Per-service copy — no shared jar
 * (microservices.md §1.4), symmetric with {@code Pii} / {@code KafkaSecurity} / {@code BoundaryHandler}.
 *
 * The triage consumer is a SYNCHRONOUS listener: the Kafka container's stop() awaits the
 * in-flight {@code process()} (≤12s LLM TimeLimiter, ai-triage-rules §5) before terminating, so
 * an in-flight triage finishes rather than being killed mid-call. The grace window
 * ({@code spring.lifecycle.timeout-per-shutdown-phase} + k8s {@code terminationGracePeriodSeconds})
 * is sized above that cap. Any alert still mid-flight at a hard kill is covered by the
 * {@code PENDING_TRIAGE} startup sweep (ai-triage-rules §2) — degraded, never re-charged.
 * This listener adds the readiness-flip + observability the other services have.
 */
@Component
public class ShutdownLifecycleListener {

    private static final Logger log = LoggerFactory.getLogger(ShutdownLifecycleListener.class);

    private final ApplicationEventPublisher events;
    private final MeterRegistry             meterRegistry;

    public ShutdownLifecycleListener(ApplicationEventPublisher events, MeterRegistry meterRegistry) {
        this.events        = events;
        this.meterRegistry = meterRegistry;
    }

    @EventListener(ContextClosedEvent.class)
    public void onContextClosed(ContextClosedEvent event) {
        log.info("Graceful shutdown initiated — readiness → REFUSING_TRAFFIC; draining in-flight triage");
        meterRegistry.counter("rpe.shutdown.initiated").increment();
        AvailabilityChangeEvent.publish(events, this, ReadinessState.REFUSING_TRAFFIC);
    }
}
