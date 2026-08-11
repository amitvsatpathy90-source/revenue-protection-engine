package io.rpe.triage.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;
import java.util.UUID;

/**
 * Inbound alert from {@code payment.alerts}.
 *
 * This is the ONLY contract shared with the core pipeline, and it is shared as a
 * schema, not as code (ai-triage-rules.md §1.4): {@code @JsonIgnoreProperties} +
 * {@code schemaVersion} give ADR-11 additive-evolution semantics. The core module's
 * {@code AlertMessage} may gain fields without touching this module.
 *
 * {@code reason} and any future free-text fields are UNTRUSTED — they enter the LLM
 * prompt only inside the fenced {@code <alert_data>} block, never the system prompt.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record PaymentAlert(
        UUID    alertId,
        String  eventId,
        String  accountId,
        String  ruleName,
        String  reason,
        Instant producedAt,
        String  schemaVersion
) {}
