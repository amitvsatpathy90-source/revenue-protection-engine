package io.rpe.ingest.mapping;

import io.rpe.ingest.mapping.CapturedPayment;

/**
 * Outcome of parsing one webhook body: a captured payment, an event we do not forward, or unusable input.
 */
public sealed interface ParsedWebhook {

    /**
     * A well-formed {@code payment.captured} event.
     */
    record Captured(CapturedPayment payment) implements ParsedWebhook {
    }

    /**
     * A valid event of any other type; acknowledged but not forwarded.
     */
    record Ignored(String eventType) implements ParsedWebhook {
    }

    /**
     * Not usable; {@code reason} is a stable code stored with the inbox row.
     */
    record Malformed(String reason) implements ParsedWebhook {
    }
}
