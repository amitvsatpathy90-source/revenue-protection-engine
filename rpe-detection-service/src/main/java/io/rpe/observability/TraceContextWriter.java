package io.rpe.observability;

import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Captures the currently-active trace context as a W3C {@link TraceCarrier} (ADR-25).
 *
 * Called on the Kafka listener thread — where Spring Kafka's consume-span observation is the
 * active context — BEFORE the event is handed to the per-account lane. The lane is a raw
 * {@code ThreadPoolExecutor} (ADR-07), so the ThreadLocal trace context does NOT follow
 * {@code lane.submit(...)}; the captured carrier is threaded explicitly through to the
 * {@code AlertIntent} instead. Explicit threading over ambient propagation is deliberate —
 * it matches how the hot path already threads {@code brokerIngestMs} and MDC, and is robust to
 * the reactive Redis hop inside the lane.
 *
 * Fail-open: no active span ⇒ {@link TraceCarrier#EMPTY} (persisted as NULL columns), never an
 * exception. Uses the same {@link Propagator} bean the W3C config wires, so the format the relay
 * later {@code extract}s is guaranteed to match what we {@code inject} here.
 */
@Component
public class TraceContextWriter {

    private final Tracer tracer;
    private final Propagator propagator;

    public TraceContextWriter(Tracer tracer, Propagator propagator) {
        this.tracer = tracer;
        this.propagator = propagator;
    }

    /** Snapshot of the active trace context, or {@link TraceCarrier#EMPTY} if none. */
    public TraceCarrier capture() {
        TraceContext context = tracer.currentTraceContext().context();
        if (context == null) {
            return TraceCarrier.EMPTY;
        }
        Map<String, String> carrier = new HashMap<>();
        propagator.inject(context, carrier, (c, key, value) -> c.put(key, value));
        return new TraceCarrier(carrier.get("traceparent"), carrier.get("tracestate"));
    }
}
