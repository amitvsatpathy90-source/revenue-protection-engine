package io.rpe.ingest.mapping;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Wire shape published to {@code payment.events}; mirrors detection's {@code PaymentEvent} contract
 * (schemaVersion "1"). Detection ignores unknown fields, so additive evolution stays safe.
 */
public record OutboundPaymentEvent(
        String eventId,
        String accountId,
        BigDecimal amount,
        BigDecimal lat,
        BigDecimal lon,
        Instant timestamp,
        String schemaVersion) {
}
