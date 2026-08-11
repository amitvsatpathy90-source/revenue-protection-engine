package io.rpe.config;

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
 * {@link ContextClosedEvent} is published at the very start of context close — BEFORE any
 * {@link org.springframework.context.SmartLifecycle#stop()} and before bean destruction. We use
 * that earliest hook to flip readiness to {@link ReadinessState#REFUSING_TRAFFIC}, so the k8s
 * readiness probe ({@code /actuator/health/readiness}) returns 503 and the pod is removed from
 * the Service endpoints before the drain proceeds. Liveness stays UP — this is "stop sending me
 * NEW work", not "I am unhealthy". Paired with the k8s {@code preStop} sleep, this gives the
 * endpoints controller time to deregister the pod ahead of the SIGTERM-driven drain.
 *
 * Observability only — it never blocks; the actual draining is done by the phase-ordered
 * SmartLifecycle beans that stop after this event.
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
        log.info("Graceful shutdown initiated — readiness → REFUSING_TRAFFIC; draining in-flight work");
        meterRegistry.counter("rpe.shutdown.initiated").increment();
        AvailabilityChangeEvent.publish(events, this, ReadinessState.REFUSING_TRAFFIC);
    }
}
