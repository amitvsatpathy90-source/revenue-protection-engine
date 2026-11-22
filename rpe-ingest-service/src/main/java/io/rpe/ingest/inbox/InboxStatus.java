package io.rpe.ingest.inbox;

/**
 * Lifecycle of an inbox row. Only PENDING rows are published; the rest are kept for audit.
 */
public enum InboxStatus {
    /**
     * Valid, fresh, awaiting the poller.
     */
    PENDING,
    /**
     * Published to payment.events by the poller.
     */
    PUBLISHED,
    /**
     * Valid event of a type we do not forward.
     */
    IGNORED,
    /**
     * Unusable input; the reason is stored.
     */
    REJECTED,
    /**
     * Delivered later than the allowed age.
     */
    STALE
}
