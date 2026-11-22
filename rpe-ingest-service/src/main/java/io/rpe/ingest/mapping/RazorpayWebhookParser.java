package io.rpe.ingest.mapping;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

import java.io.IOException;

/**
 * Extracts the fields the mapper needs from a gateway webhook body. Tolerant of everything it does not read:
 * unknown fields are ignored, and {@code notes} may be an object or an empty array (the gateway serializes an
 * empty notes map as {@code []}). Never throws on bad input; returns {@link ParsedWebhook.Malformed}.
 */
public final class RazorpayWebhookParser {

    private static final String CAPTURED = "payment.captured";
    private static final ObjectMapper MAPPER = JsonMapper.builder().build();   // thread-safe once configured

    /**
     * @param rawBody webhook body bytes; the signature has already been verified by the caller
     */
    public ParsedWebhook parse(byte[] rawBody) {
        JsonNode root;
        try {
            root = MAPPER.readTree(rawBody);
        } catch (IOException e) {
            return new ParsedWebhook.Malformed("unparseable-payload");
        }
        if (root == null || !root.isObject()) {
            return new ParsedWebhook.Malformed("unparseable-payload");
        }

        String eventType = scalarText(root.get("event"));
        if (eventType == null || eventType.isBlank()) {
            return new ParsedWebhook.Malformed("missing-event-type");
        }
        if (!CAPTURED.equals(eventType)) {
            return new ParsedWebhook.Ignored(eventType);
        }

        JsonNode entity = root.path("payload").path("payment").path("entity");
        if (!entity.isObject()) {
            return new ParsedWebhook.Malformed("missing-payment-entity");
        }
        JsonNode createdAt = root.get("created_at");
        if (!isLong(createdAt)) {
            return new ParsedWebhook.Malformed("missing-event-created-at");
        }
        JsonNode amount = entity.get("amount");
        if (!isLong(amount)) {
            return new ParsedWebhook.Malformed("invalid-amount");
        }

        JsonNode notes = entity.get("notes");
        return new ParsedWebhook.Captured(new CapturedPayment(scalarText(entity.get("id")),
                scalarText(entity.get("currency")),
                amount.longValue(),
                noteText(notes, "accountId"),
                noteText(notes, "lat"),
                noteText(notes, "lon"),
                createdAt.longValue()));
    }

    /**
     * True for an integral JSON number that fits a long (a fractional amount is not a valid minor-unit value).
     */
    private static boolean isLong(JsonNode n) {
        return n != null && n.isIntegralNumber() && n.canConvertToLong();
    }

    /**
     * Text of a scalar node; null when absent, JSON null or not a scalar.
     */
    private static String scalarText(JsonNode n) {
        return (n == null || n.isNull() || !n.isValueNode()) ? null : n.asText();
    }

    /**
     * Value of {@code notes[key]}. A non-object notes (including the gateway's empty {@code []}) or a missing
     * key yields null; a present but non-scalar value yields "" so the mapper rejects it instead of ignoring it.
     */
    private static String noteText(JsonNode notes, String key) {
        if (notes == null || !notes.isObject()) {
            return null;
        }
        JsonNode v = notes.get(key);
        if (v == null || v.isNull()) {
            return null;
        }
        return v.isValueNode() ? v.asText() : "";
    }
}
