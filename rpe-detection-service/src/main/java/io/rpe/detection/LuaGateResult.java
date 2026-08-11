package io.rpe.detection;

import java.util.List;

/**
 * Populated from the Lua gate's raw return array (ADR-14).
 *
 * Lua returns: [dedupBlocked, velocityPriorCount, welfordCount, welfordMean, welfordM2,
 *               geoPrevLat|nil, geoPrevLon|nil, geoPrevBrokerMs|nil]
 *
 * When {@code dedupBlocked} is true, the gate returned at its step-0 EXISTS pre-check having mutated
 * NOTHING (velocity/Welford/geo are only touched for a first-seen event_id), so a Kafka at-least-once
 * redelivery no longer perturbs the Welford baseline. The other fields carry the current read-only state
 * and are ignored by callers when blocked.
 *
 * {@link Detector#evaluate} is called ONLY when {@code dedupBlocked == false}.
 */
public record LuaGateResult(
        boolean     dedupBlocked,
        long        velocityPriorCount,
        long        welfordCount,
        double      welfordMean,
        double      welfordM2,
        GeoSnapshot geoPrev   // null = no prior location for this account
) {
    /**
     * Maps the raw Lua multi-bulk reply. This is the single site where {@link GeoSnapshot}
     * is assembled (ADR-14). Spring Data Redis maps Redis integer replies to Long and nil
     * bulk replies to null.
     *
     * Partial geo state (some-but-not-all of the three fields present) is a gate contract
     * violation and THROWS — it is never silently coerced to a null {@code geoPrev}.
     * Silent coercion would turn a Lua regression into invisible missed detections.
     *
     * @throws IllegalArgumentException geo contract violation — partial fields, NaN,
     *         out-of-range, or non-positive brokerMs (includes geo field parse failures)
     * @throws IllegalStateException    reply shape violation — wrong size or wrong types
     *         in the non-geo fields. Distinct type so the caller can tag the violation
     *         counter {@code reason=shape} vs {@code reason=geo} without string matching.
     */
    public static LuaGateResult from(List<Object> raw) {
        boolean blocked;
        long    prior;
        long    count;
        double  mean;
        double  m2;
        try {
            blocked = ((Long) raw.get(0)) == 1L;
            prior   = (Long) raw.get(1);
            count   = (Long) raw.get(2);
            mean    = Double.parseDouble((String) raw.get(3));
            m2      = Double.parseDouble((String) raw.get(4));
        } catch (RuntimeException e) {
            // Exception type only — raw reply values never appear in the message
            throw new IllegalStateException(
                    "gate reply shape invalid: " + e.getClass().getSimpleName(), e);
        }

        Object rawLat = elementAt(raw, 5);
        Object rawLon = elementAt(raw, 6);
        Object rawTs  = elementAt(raw, 7);

        GeoSnapshot geo = null;
        boolean latPresent = rawLat != null;
        boolean lonPresent = rawLon != null;
        boolean tsPresent  = rawTs  != null;
        if (latPresent || lonPresent || tsPresent) {
            if (!(latPresent && lonPresent && tsPresent)) {
                throw new IllegalArgumentException(
                        "partial geo state: latPresent=%b lonPresent=%b tsPresent=%b"
                                .formatted(latPresent, lonPresent, tsPresent));
            }
            try {
                geo = new GeoSnapshot(
                        Double.parseDouble((String) rawLat),
                        Double.parseDouble((String) rawLon),
                        Long.parseLong((String) rawTs));
            } catch (ClassCastException e) {
                throw new IllegalArgumentException("geo field has non-string type", e);
            }
            // NumberFormatException extends IllegalArgumentException — already the geo bucket.
            // GeoSnapshot guard failures (NaN / range / brokerMs) propagate as-is.
        }

        return new LuaGateResult(blocked, prior, count, mean, m2, geo);
    }

    private static Object elementAt(List<Object> raw, int index) {
        // Lua nil-terminated arrays may arrive truncated: { a, b, nil, nil } has size 2.
        // A missing trailing element is equivalent to an explicit nil.
        return index < raw.size() ? raw.get(index) : null;
    }
}
