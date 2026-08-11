package io.rpe.relay.config;

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
 * {@link ContextClosedEvent} fires at the very start of context close — before any
 * {@link org.springframework.context.SmartLifecycle#stop()} (including the relay-loop stop) and
 * before bean destruction. Flipping readiness to {@link ReadinessState#REFUSING_TRAFFIC} here
 * makes the k8s readiness probe return 503 so the pod is removed from Service endpoints before
 * the drain proceeds. Liveness stays UP. Observability only — never blocks.
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
        log.info("Graceful shutdown initiated — readiness → REFUSING_TRAFFIC; stopping relay loop");
        meterRegistry.counter("rpe.shutdown.initiated").increment();
        AvailabilityChangeEvent.publish(events, this, ReadinessState.REFUSING_TRAFFIC);
    }
}
