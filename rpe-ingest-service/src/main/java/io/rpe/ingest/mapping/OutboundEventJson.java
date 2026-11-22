package io.rpe.ingest.mapping;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Serializes {@link OutboundPaymentEvent} to the JSON detection consumes. Built field by field so the wire
 * format is explicit: {@code timestamp} is an ISO-8601 instant string, numbers are JSON numbers.
 */
public final class OutboundEventJson {

    private static final ObjectMapper MAPPER = JsonMapper.builder().build();

    private OutboundEventJson() {
    }

    public static String toJson(OutboundPaymentEvent e) {
        ObjectNode n = MAPPER.createObjectNode();
        n.put("eventId", e.eventId());
        n.put("accountId", e.accountId());
        n.put("amount", e.amount());
        n.put("lat", e.lat());
        n.put("lon", e.lon());
        n.put("timestamp", e.timestamp().toString());
        n.put("schemaVersion", e.schemaVersion());
        return n.toString();
    }
}
