package io.rpe.alert.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;
import java.util.UUID;

/**
 * Inbound alert consumed from {@code payment.alerts}.
 *
 * This is a SCHEMA contract, not shared code (ADR-17 §3.3 / microservices.md §2): the
 * alert-service declares its own copy of the alert message rather than importing the
 * detection service's {@code AlertMessage}. {@code @JsonIgnoreProperties(ignoreUnknown =
 * true)} gives ADR-11 additive-evolution semantics — the producer may add fields without
 * breaking this consumer. Fields mirror the wire format the relay republishes.
 *
 * {@code producedAt} = original event time (business ordering). Operational time is recorded
 * separately as {@code processed_alerts.acted_at} (DEFAULT now()) — never conflate the two.
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
