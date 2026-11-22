package io.rpe.ingest.inbox;

/**
 * Durable store behind the webhook acknowledgement (ADR-33 item 3).
 */
public interface InboxRepository {

    /**
     * Stores the row unless a uniqueness constraint already holds it.
     *
     * @return true if inserted; false if the event id was already stored, or the payment is already forwarded
     * @throws RuntimeException on storage failure; the caller must not acknowledge the webhook
     */
    boolean insert(InboxRecord inboxRecord);
}
