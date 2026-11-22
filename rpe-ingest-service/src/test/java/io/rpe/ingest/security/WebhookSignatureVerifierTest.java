package io.rpe.ingest.security;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Signature vectors were computed independently with Python's hmac module and RFC 4231 test case 2.
 */
class WebhookSignatureVerifierTest {

    private static final String SECRET = "whsec_test";
    private static final String BODY =
            "{\"entity\":\"event\",\"event\":\"payment.captured\",\"created_at\":1691735748}";
    private static final String BODY_SIGNATURE = "8f0477f765a2a04b572079661a77e0350ce34d3ea089d75d774c0337cc5edf98";

    private final WebhookSignatureVerifier verifier = new WebhookSignatureVerifier(SECRET);

    @Test
    void acceptsValidSignature() {
        assertTrue(verifier.verify(bytes(BODY), BODY_SIGNATURE));
    }

    @Test
        // From RFC 4231 §4.2, Key is "Jefe".
    void matchesRfc4231Vector() {
        var rfc = new WebhookSignatureVerifier("Jefe");
        assertTrue(rfc.verify(bytes("what do ya want for nothing?"),
                "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"));
    }

    @Test
    void acceptsUppercaseHex() {
        assertTrue(verifier.verify(bytes(BODY), BODY_SIGNATURE.toUpperCase()));
    }

    @Test
    void rejectsTamperedBody() {
        assertFalse(verifier.verify(bytes(BODY.replace("1691735748", "1691735749")), BODY_SIGNATURE));
    }

    @Test
    void rejectsReserializedBody() {
        // pretty-printed JSON is semantically equal but not byte-equal: the digest must not match
        assertFalse(verifier.verify(bytes(BODY.replace(",", ", ")), BODY_SIGNATURE));
    }

    @Test
    void rejectsWrongSecret() {
        assertFalse(new WebhookSignatureVerifier("other").verify(bytes(BODY), BODY_SIGNATURE));
    }

    @Test
    void rejectsMalformedSignatures() {
        assertFalse(verifier.verify(bytes(BODY), null));
        assertFalse(verifier.verify(bytes(BODY), ""));
        assertFalse(verifier.verify(bytes(BODY), BODY_SIGNATURE.substring(1)));
        assertFalse(verifier.verify(bytes(BODY), "z".repeat(64)));
        assertFalse(verifier.verify(null, BODY_SIGNATURE));
    }

    @Test
    void refusesBlankSecret() {
        assertThrows(IllegalArgumentException.class, () -> new WebhookSignatureVerifier(null));
        assertThrows(IllegalArgumentException.class, () -> new WebhookSignatureVerifier(""));
        assertThrows(IllegalArgumentException.class, () -> new WebhookSignatureVerifier("   "));
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
