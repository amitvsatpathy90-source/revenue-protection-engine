package io.rpe.ingest.service;

/**
 * Outcome of one webhook; the controller maps it to an HTTP status.
 */
public enum IngestResult {
    /**
     * Stored (forwardable or not); respond 2xx.
     */
    ACCEPTED,
    /**
     * Already stored (redelivery or same payment); respond 2xx so the gateway stops retrying.
     */
    DUPLICATE,
    /**
     * Missing or wrong signature; respond 401.
     */
    UNAUTHORIZED,
    /**
     * Authentic but lacks the delivery id needed for dedup; respond 400.
     */
    BAD_REQUEST
}
