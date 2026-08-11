package io.rpe.detection;

/**
 * Previous geo location for an account, as returned by the Lua gate.
 *
 * All three fields are unconditionally valid when this record is non-null.
 * "No prior location" is encoded at the reference level (null GeoSnapshot),
 * not with NaN or Long.MIN_VALUE sentinels — heterogeneous sentinels across
 * three flat fields make desync representable in the type (ADR-14).
 *
 * {@code brokerMs} is the broker ingest timestamp of the prior event,
 * NOT the payload timestamp — payload clocks drift.
 *
 * Exception messages describe violation shape (boolean flags) — never coordinate
 * values. {@code getMessage()} outlives the log sink: it surfaces in span events,
 * aggregator fingerprints, and rethrown exception chains where MDC and log
 * redaction do not travel. Coordinates are PII; {@code brokerMs} alone is not.
 */
public record GeoSnapshot(double lat, double lon, long brokerMs) {

    public GeoSnapshot {
        // 1. NaN BEFORE range — NaN comparisons are silently false; the range check
        //    alone passes NaN through. This ordering is load-bearing: do not reorder.
        if (Double.isNaN(lat) || Double.isNaN(lon)) {
            throw new IllegalArgumentException(
                    "NaN coordinate(s): latNaN=%b lonNaN=%b".formatted(
                            Double.isNaN(lat), Double.isNaN(lon)));
        }
        // 2. Range — also catches ±Infinity and sentinel values (e.g. 999).
        //    Message uses ok-flags only — never a coordinate value.
        if (lat < -90.0 || lat > 90.0 || lon < -180.0 || lon > 180.0) {
            throw new IllegalArgumentException(
                    "coordinate out of range: latOk=%b lonOk=%b".formatted(
                            lat >= -90.0 && lat <= 90.0, lon >= -180.0 && lon <= 180.0));
        }
        // 3. brokerMs — rejects Kafka's -1 no-timestamp sentinel and epoch-0.
        //    A timestamp without identity is not PII; it may appear verbatim.
        if (brokerMs <= 0) {
            throw new IllegalArgumentException("non-positive brokerMs: " + brokerMs);
        }
    }
}
