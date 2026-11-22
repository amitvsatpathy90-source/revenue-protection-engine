package io.rpe.ingest.inbox;

import java.time.Instant;

/**
 * One row to store.
 *
 * @param eventId     gateway delivery id ({@code x-razorpay-event-id}); unique
 * @param eventType   gateway event name, or "unknown" when the body was unusable
 * @param paymentId   payment id when known (unique among forwarded rows), else null
 * @param accountId   account when known, else null
 * @param status      lifecycle state
 * @param reason      stable reason code for REJECTED/STALE rows, else null
 * @param payloadJson outbound event JSON for PENDING rows, else null
 * @param receivedAt  server-side receive time; becomes the Kafka record timestamp (ADR-33 item 8)
 */
public record InboxRecord(
        String eventId,
        String eventType,
        String paymentId,
        String accountId,
        InboxStatus status,
        String reason,
        String payloadJson,
        Instant receivedAt) {
}
