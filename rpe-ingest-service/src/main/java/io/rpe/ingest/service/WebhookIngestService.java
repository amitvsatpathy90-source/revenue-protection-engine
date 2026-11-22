package io.rpe.ingest.service;

import io.rpe.ingest.inbox.InboxRecord;
import io.rpe.ingest.inbox.InboxRepository;
import io.rpe.ingest.inbox.InboxStatus;
import io.rpe.ingest.mapping.CapturedPayment;
import io.rpe.ingest.mapping.MappingOutcome;
import io.rpe.ingest.mapping.OutboundEventJson;
import io.rpe.ingest.mapping.ParsedWebhook;
import io.rpe.ingest.mapping.PaymentEventMapper;
import io.rpe.ingest.mapping.RazorpayWebhookParser;
import io.rpe.ingest.security.WebhookSignatureVerifier;

import java.time.Instant;

/**
 * Webhook pipeline (ADR-33 items 2-7): verify signature, parse, map, store. A storage failure propagates, so
 * the gateway receives a 5xx and retries; every authentic, storable outcome (including ignored, rejected and
 * stale events) is acknowledged, because retrying cannot change it.
 */
public final class WebhookIngestService {

    private static final String UNKNOWN_EVENT = "unknown";

    private final WebhookSignatureVerifier verifier;
    private final RazorpayWebhookParser parser;
    private final PaymentEventMapper mapper;
    private final InboxRepository inbox;

    public WebhookIngestService(WebhookSignatureVerifier verifier, RazorpayWebhookParser parser,
                                PaymentEventMapper mapper, InboxRepository inbox) {
        this.verifier = verifier;
        this.parser = parser;
        this.mapper = mapper;
        this.inbox = inbox;
    }

    /**
     * @param rawBody    body bytes exactly as received
     * @param signature  {@code X-Razorpay-Signature} header, may be null
     * @param eventId    {@code x-razorpay-event-id} header, may be null
     * @param receivedAt server-side receive time
     */
    public IngestResult ingest(byte[] rawBody, String signature, String eventId, Instant receivedAt) {
        if (!verifier.verify(rawBody, signature)) {
            return IngestResult.UNAUTHORIZED;   // checked first: nothing else is trusted before this
        }
        if (eventId == null || eventId.isBlank()) {
            return IngestResult.BAD_REQUEST;    // without the delivery id duplicates cannot be detected
        }
        InboxRecord record = toRecord(parser.parse(rawBody), eventId, receivedAt);
        return inbox.insert(record) ? IngestResult.ACCEPTED : IngestResult.DUPLICATE;
    }

    private InboxRecord toRecord(ParsedWebhook parsed, String eventId, Instant receivedAt) {
        return switch (parsed) {
            case ParsedWebhook.Ignored i ->
                    new InboxRecord(eventId, i.eventType(), null, null, InboxStatus.IGNORED, null, null, receivedAt);
            case ParsedWebhook.Malformed m -> new InboxRecord(eventId,
                    UNKNOWN_EVENT,
                    null,
                    null,
                    InboxStatus.REJECTED,
                    m.reason(),
                    null,
                    receivedAt);
            case ParsedWebhook.Captured c ->
                    fromOutcome(eventId, c.payment(), mapper.map(c.payment(), receivedAt), receivedAt);
        };
    }

    private static InboxRecord fromOutcome(String eventId, CapturedPayment p,
                                           MappingOutcome outcome, Instant receivedAt) {
        String type = "payment.captured";
        return switch (outcome) {
            case MappingOutcome.Forward f -> new InboxRecord(eventId, type, f.event().eventId(), f.event().accountId(),
                    InboxStatus.PENDING, null, OutboundEventJson.toJson(f.event()), receivedAt);
            case MappingOutcome.Rejected r -> new InboxRecord(eventId, type, p.paymentId(), p.accountId(),
                    InboxStatus.REJECTED, r.reason(), null, receivedAt);
            case MappingOutcome.Stale s -> new InboxRecord(eventId, type, p.paymentId(), p.accountId(),
                    InboxStatus.STALE, "stale-event age=" + s.age().toSeconds() + "s", null, receivedAt);
        };
    }
}
