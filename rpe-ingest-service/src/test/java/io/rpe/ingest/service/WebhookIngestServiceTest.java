package io.rpe.ingest.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.rpe.ingest.inbox.InboxRecord;
import io.rpe.ingest.inbox.InboxRepository;
import io.rpe.ingest.inbox.InboxStatus;
import io.rpe.ingest.mapping.PaymentEventMapper;
import io.rpe.ingest.mapping.RazorpayWebhookParser;
import io.rpe.ingest.security.WebhookSignatureVerifier;
import io.rpe.ingest.testsupport.WebhookBodies;
import org.junit.jupiter.api.Test;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WebhookIngestServiceTest {

    private static final Instant ON_TIME = Instant.ofEpochSecond(WebhookBodies.EVENT_EPOCH + 30);
    private static final String EVENT_ID = "evt_1";

    /**
     * In-memory stand-in; the real uniqueness constraints are exercised against PostgreSQL separately.
     */
    private static final class FakeInbox implements InboxRepository {
        final List<InboxRecord> rows = new ArrayList<>();
        private final Set<String> eventIds = new HashSet<>();
        boolean failing;

        @Override
        public boolean insert(InboxRecord record) {
            if (failing) {
                throw new IllegalStateException("db down");
            }
            if (!eventIds.add(record.eventId())) {
                return false;
            }
            rows.add(record);
            return true;
        }
    }

    private final FakeInbox inbox = new FakeInbox();
    private final WebhookIngestService service = new WebhookIngestService(
            new WebhookSignatureVerifier(WebhookBodies.SECRET),
            new RazorpayWebhookParser(),
            new PaymentEventMapper(new PaymentEventMapper.Config(
                    Duration.ofMinutes(15), Duration.ofMinutes(5),
                    List.of(new PaymentEventMapper.GeoPoint(
                            new BigDecimal("28.6139"),
                            new BigDecimal("77.2090"))),
                    "INR")),
            inbox);

    private IngestResult ingest(String body, String signature, String eventId, Instant at) {
        return service.ingest(body.getBytes(StandardCharsets.UTF_8), signature, eventId, at);
    }

    @Test
    void storesValidCapturedAsPendingWithOutboundJson() throws Exception {
        assertEquals(IngestResult.ACCEPTED,
                ingest(WebhookBodies.captured(), WebhookBodies.CAPTURED_SIG, EVENT_ID, ON_TIME));

        var row = inbox.rows.get(0);
        assertEquals(InboxStatus.PENDING, row.status());
        assertEquals("pay_DESlfW9H8K9uqM", row.paymentId());
        assertEquals(ON_TIME, row.receivedAt());
        JsonNode json = new ObjectMapper().readTree(row.payloadJson());
        assertEquals("pay_DESlfW9H8K9uqM", json.get("eventId").asText());
        assertEquals("acc-smoke-1", json.get("accountId").asText());
        assertEquals(0, new BigDecimal("299.00").compareTo(json.get("amount").decimalValue()));
        assertEquals("2023-08-11T06:35:48Z", json.get("timestamp").asText());
        assertEquals("1", json.get("schemaVersion").asText());
    }

    @Test
    void rejectsBadOrMissingSignatureBeforeAnythingElse() {
        assertEquals(IngestResult.UNAUTHORIZED, ingest(WebhookBodies.captured(), "0".repeat(64), EVENT_ID, ON_TIME));
        assertEquals(IngestResult.UNAUTHORIZED, ingest(WebhookBodies.captured(), null, EVENT_ID, ON_TIME));
        // signature is checked before the event id, so a bad signature wins over a missing id
        assertEquals(IngestResult.UNAUTHORIZED, ingest(WebhookBodies.captured(), "0".repeat(64), null, ON_TIME));
        assertTrue(inbox.rows.isEmpty());
    }

    @Test
    void authenticButWithoutEventIdIsBadRequest() {
        assertEquals(IngestResult.BAD_REQUEST,
                ingest(WebhookBodies.captured(), WebhookBodies.CAPTURED_SIG, null, ON_TIME));
        assertEquals(IngestResult.BAD_REQUEST,
                ingest(WebhookBodies.captured(), WebhookBodies.CAPTURED_SIG, "  ", ON_TIME));
        assertTrue(inbox.rows.isEmpty());
    }

    @Test
    void otherEventTypesAreAcknowledgedAndIgnored() {
        assertEquals(IngestResult.ACCEPTED,
                ingest(WebhookBodies.authorized(), WebhookBodies.AUTHORIZED_SIG, EVENT_ID, ON_TIME));
        var row = inbox.rows.get(0);
        assertEquals(InboxStatus.IGNORED, row.status());
        assertEquals("payment.authorized", row.eventType());
        assertNull(row.payloadJson());
    }

    @Test
    void missingAccountIsStoredRejectedNotForwarded() {
        assertEquals(IngestResult.ACCEPTED,
                ingest(WebhookBodies.capturedEmptyNotes(), WebhookBodies.CAPTURED_EMPTY_NOTES_SIG, EVENT_ID, ON_TIME));
        var row = inbox.rows.get(0);
        assertEquals(InboxStatus.REJECTED, row.status());
        assertEquals("missing-account-id", row.reason());
        assertNull(row.payloadJson());
    }

    @Test
    void lateDeliveryIsStoredStale() {
        var late = Instant.ofEpochSecond(WebhookBodies.EVENT_EPOCH).plus(Duration.ofMinutes(20));
        assertEquals(IngestResult.ACCEPTED,
                ingest(WebhookBodies.captured(), WebhookBodies.CAPTURED_SIG, EVENT_ID, late));
        var row = inbox.rows.get(0);
        assertEquals(InboxStatus.STALE, row.status());
        assertEquals("stale-event age=1200s", row.reason());
        assertNull(row.payloadJson());
    }

    @Test
    void redeliveryOfSameEventIsDuplicate() {
        assertEquals(IngestResult.ACCEPTED,
                ingest(WebhookBodies.captured(), WebhookBodies.CAPTURED_SIG, EVENT_ID, ON_TIME));
        assertEquals(IngestResult.DUPLICATE,
                ingest(WebhookBodies.captured(), WebhookBodies.CAPTURED_SIG, EVENT_ID, ON_TIME));
        assertEquals(1, inbox.rows.size());
    }

    @Test
    void authenticUnparseableBodyIsStoredRejected() throws Exception {
        var body = "not json at all";
        assertEquals(IngestResult.ACCEPTED, ingest(body, sign(body), EVENT_ID, ON_TIME));
        var row = inbox.rows.get(0);
        assertEquals(InboxStatus.REJECTED, row.status());
        assertEquals("unparseable-payload", row.reason());
        assertEquals("unknown", row.eventType());
    }

    @Test
    void storageFailurePropagatesSoTheGatewayRetries() {
        inbox.failing = true;
        assertThrows(IllegalStateException.class,
                () -> ingest(WebhookBodies.captured(), WebhookBodies.CAPTURED_SIG, EVENT_ID, ON_TIME));
    }

    private static String sign(String body) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(WebhookBodies.SECRET.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        return HexFormat.of().formatHex(mac.doFinal(body.getBytes(StandardCharsets.UTF_8)));
    }
}
