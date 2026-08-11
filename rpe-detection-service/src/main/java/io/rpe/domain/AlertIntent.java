package io.rpe.domain;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.UUID;

/**
 * Submitted to the outbox batch writer.
 *
 * {@code payload} is the {@link AlertMessage} serialized as {@code JsonNode} for outbox storage.
 * The relay treats it as opaque — it never re-deserializes to AlertMessage (ADR-11).
 * This decouples relay from DTO evolution: old outbox rows relay cleanly regardless of schema
 * changes to AlertMessage.
 *
 * {@code traceparent}/{@code tracestate} carry the W3C trace context of the originating event's
 * consume span (ADR-25), persisted on the outbox row so the relay can continue the same trace
 * across the durable-queue hop. Both nullable: NULL when tracing is off or no span was active.
 * They are trace metadata, NOT part of the alert contract — the relay forwards {@code payload}
 * byte-identically and reads these columns separately.
 */
public record AlertIntent(
        UUID     alertId,
        String   accountId,
        JsonNode payload,
        Instant  createdAt,
        String   traceparent,
        String   tracestate
) {}
