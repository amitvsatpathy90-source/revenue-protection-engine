package io.rpe.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;
import java.util.UUID;

/**
 * Payload published on {@code payment.alerts}.
 *
 * {@code producedAt} = original event time from the PaymentEvent payload.
 * Use for business ordering (when the transaction occurred).
 *
 * {@code actedAt} is recorded only in {@code processed_alerts.acted_at} (DEFAULT now()).
 * Never conflate with producedAt — one is business time, the other is operational time.
 *
 * {@code @JsonIgnoreProperties(ignoreUnknown = true)}: outbox rows written by a newer relay
 * may carry extra fields. Old consumers must not fail on fields they don't know about.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record AlertMessage(
        UUID    alertId,
        String  eventId,
        String  accountId,
        String  ruleName,
        String  reason,
        Instant producedAt
) {}
