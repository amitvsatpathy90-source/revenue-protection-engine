package io.rpe.redis;

/**
 * The Lua gate returned state that violates the ADR-14 contract — partial geo fields,
 * out-of-range coordinates, or a malformed reply shape.
 *
 * Deterministic per event: retrying produces the same violation, so this routes
 * directly to {@code payment.events.DLT} with no retry. The throw site in
 * {@link RedisGate} owns the {@code rpe.gate.invalid_state} counter increment;
 * the DLT terminal handler treats this exception as pre-reported and logs at
 * {@code debug} only — one owner, one emission (ADR-14).
 *
 * {@code reason} is a bounded metric tag value ({@code geo} | {@code shape}),
 * never free text. The message carries only the masked event id — coordinates
 * and amounts never appear in exception messages.
 */
public class GateContractViolation extends RuntimeException {

    private final String reason;

    public GateContractViolation(String maskedEventId, String reason, Throwable cause) {
        super("gate contract violation (reason=%s) event=...%s".formatted(reason, maskedEventId), cause);
        this.reason = reason;
    }

    /** Bounded tag value used for {@code rpe.gate.invalid_state{reason=...}}. */
    public String reason() {
        return reason;
    }
}
