package io.rpe.alert.config;

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
 * The alert consumer is a SYNCHRONOUS listener: the Kafka container's stop() already awaits the
 * in-flight {@code consume()} (insert → ack) before the container terminates, so no custom drain
 * bean is needed here. This listener adds the same readiness-flip + observability the other
 * services have: {@link ContextClosedEvent} fires first → readiness {@link ReadinessState#REFUSING_TRAFFIC}
 * → pod removed from Service endpoints before the container drains. Liveness stays UP.
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
        log.info("Graceful shutdown initiated — readiness → REFUSING_TRAFFIC; draining in-flight alert");
        meterRegistry.counter("rpe.shutdown.initiated").increment();
        AvailabilityChangeEvent.publish(events, this, ReadinessState.REFUSING_TRAFFIC);
    }
}
