package io.rpe.observability;

/**
 * The W3C trace context captured from the active consume span, in the two header fields the
 * relay needs to continue the trace across the outbox durable-queue hop (ADR-25):
 * {@code traceparent} (trace-id + parent-span-id + flags) and {@code tracestate} (vendor state,
 * usually empty in a single-vendor OTel setup).
 *
 * Both fields are nullable: when tracing is disabled, no span is active, or the inbound event
 * carried no trace context, {@link #EMPTY} is used and the persisted outbox columns are NULL —
 * the relay then starts a fresh trace for that row (fail-open, never an error).
 */
public record TraceCarrier(String traceparent, String tracestate) {

    public static final TraceCarrier EMPTY = new TraceCarrier(null, null);

    public boolean isPresent() {
        return traceparent != null && !traceparent.isBlank();
    }
}
