package io.rpe.ingest.mapping;

import io.rpe.ingest.testsupport.WebhookBodies;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

class RazorpayWebhookParserTest {

    private final RazorpayWebhookParser parser = new RazorpayWebhookParser();

    private ParsedWebhook parse(String body) {
        return parser.parse(body.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void parsesCapturedPayment() {
        var c = assertInstanceOf(ParsedWebhook.Captured.class, parse(WebhookBodies.captured()));
        var p = c.payment();
        assertEquals("pay_DESlfW9H8K9uqM", p.paymentId());
        assertEquals("INR", p.currency());
        assertEquals(29_900L, p.amountMinor());
        assertEquals("acc-smoke-1", p.accountId());
        assertNull(p.latNote());
        assertNull(p.lonNote());
        assertEquals(WebhookBodies.EVENT_EPOCH, p.eventCreatedAtEpochSec());   // event-level, not payment-level
    }

    @Test
    void acceptsEmptyArrayNotes() {
        var c = assertInstanceOf(ParsedWebhook.Captured.class, parse(WebhookBodies.capturedEmptyNotes()));
        assertNull(c.payment().accountId());
    }

    @Test
    void readsScenarioOverrideNotesAsText() {
        var body = WebhookBodies.payment("payment.captured", "captured",
                "{\"accountId\":\"acc-1\",\"lat\":51.5072,\"lon\":\"-0.1276\"}");
        var p = assertInstanceOf(ParsedWebhook.Captured.class, parse(body)).payment();
        assertEquals("51.5072", p.latNote());      // numeric note kept as text
        assertEquals("-0.1276", p.lonNote());
    }

    @Test
    void nonScalarNoteBecomesBlankSoMapperRejectsIt() {
        var body = WebhookBodies.payment("payment.captured", "captured", "{\"accountId\":{\"x\":1}}");
        var p = assertInstanceOf(ParsedWebhook.Captured.class, parse(body)).payment();
        assertEquals("", p.accountId());
    }

    @Test
    void otherEventTypesAreIgnored() {
        var ignored = assertInstanceOf(ParsedWebhook.Ignored.class, parse(WebhookBodies.authorized()));
        assertEquals("payment.authorized", ignored.eventType());
    }

    @Test
    void malformedInputsReturnReasonCodes() {
        assertEquals(new ParsedWebhook.Malformed("unparseable-payload"), parse("not json"));
        assertEquals(new ParsedWebhook.Malformed("unparseable-payload"), parse("[1,2]"));
        assertEquals(new ParsedWebhook.Malformed("unparseable-payload"), parse(""));
        assertEquals(new ParsedWebhook.Malformed("missing-event-type"), parse("{\"created_at\":1}"));
        assertEquals(new ParsedWebhook.Malformed("missing-payment-entity"),
                parse("{\"event\":\"payment.captured\",\"created_at\":1}"));
        assertEquals(new ParsedWebhook.Malformed("missing-event-created-at"),
                parse(WebhookBodies.captured().replace("\"created_at\":1691735748", "\"created_at\":\"x\"")));
        assertEquals(new ParsedWebhook.Malformed("invalid-amount"),
                parse(WebhookBodies.captured().replace("\"amount\":29900", "\"amount\":299.5")));
        assertEquals(new ParsedWebhook.Malformed("invalid-amount"),
                parse(WebhookBodies.captured().replace("\"amount\":29900,", "")));
    }
}
