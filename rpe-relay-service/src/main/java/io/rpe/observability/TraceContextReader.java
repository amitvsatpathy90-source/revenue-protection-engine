package io.rpe.observability;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Restores the W3C trace context persisted on an outbox row (ADR-25) and runs the publish under
 * it, so the relay's {@code payment.alerts} produce span continues the originating event's trace
 * across the durable-queue gap — the seam the k8s OTel agent cannot stitch (it sees detection's
 * INSERT and the relay's SELECT as unrelated ops).
 *
 * This is the relay-side counterpart of detection's {@code TraceContextWriter} — a per-service
 * copy (microservices.md §1.4: no shared jar), symmetric with {@code Pii}/{@code KafkaSecurity}/
 * {@code BoundaryHandler}.
 *
 * Fail-open: a NULL/blank traceparent (legacy row, or a producer that ran without tracing) runs
 * the action with no restored parent — the observation-enabled template then starts a fresh trace
 * for that alert. Never throws into the publish path.
 */
@Component
public class TraceContextReader {

    private final Tracer tracer;
    private final Propagator propagator;

    public TraceContextReader(Tracer tracer, Propagator propagator) {
        this.tracer = tracer;
        this.propagator = propagator;
    }

    /**
     * Runs {@code action} inside a span whose parent is the persisted W3C context. With that span
     * current, the observation-enabled relay {@code KafkaTemplate} emits the produce span as its
     * child and injects {@code traceparent} into the outgoing record's headers.
     */
    public <T> T continueTrace(String traceparent, String tracestate, String spanName, Supplier<T> action) {
        if (traceparent == null || traceparent.isBlank()) {
            return action.get();
        }
        Map<String, String> carrier = new HashMap<>();
        carrier.put("traceparent", traceparent);
        if (tracestate != null && !tracestate.isBlank()) {
            carrier.put("tracestate", tracestate);
        }
        Span span = propagator.extract(carrier, Map::get).name(spanName).start();
        try (Tracer.SpanInScope ignored = tracer.withSpan(span)) {
            return action.get();
        } finally {
            span.end();
        }
    }
}
