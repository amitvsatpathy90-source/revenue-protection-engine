package io.rpe.ingest.security;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.HexFormat;

/**
 * Verifies the gateway's {@code X-Razorpay-Signature}: a hex HMAC-SHA256 of the raw request bytes,
 * keyed by the webhook secret. Callers must pass the bytes exactly as received; re-serialized
 * JSON changes the digest. Thread-safe: the key is immutable and a {@link Mac} is created per call.
 */
public final class WebhookSignatureVerifier {

    private static final String HMAC = "HmacSHA256";
    private static final int SIGNATURE_HEX_LENGTH = 64;   // 32-byte digest, hex encoded

    private final SecretKeySpec key;

    /**
     * @param secret webhook secret configured in the gateway dashboard
     * @throws IllegalArgumentException if the secret is null or blank (refuses to run unauthenticated)
     */
    public WebhookSignatureVerifier(String secret) {
        if (secret == null || secret.isBlank()) {
            throw new IllegalArgumentException("ingest.razorpay.webhook-secret must be set");
        }
        this.key = new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), HMAC);
    }

    /**
     * @param rawBody         request body bytes exactly as received
     * @param signatureHeader value of {@code X-Razorpay-Signature}; hex, case-insensitive
     * @return true only if the header is a well-formed digest equal to the computed HMAC
     */
    public boolean verify(byte[] rawBody, String signatureHeader) {
        if (rawBody == null || signatureHeader == null || signatureHeader.length() != SIGNATURE_HEX_LENGTH) {
            return false;
        }
        byte[] provided;
        try {
            provided = HexFormat.of().parseHex(signatureHeader);
        } catch (IllegalArgumentException notHex) {
            return false;
        }
        // MessageDigest.isEqual is constant-time for equal-length inputs
        return MessageDigest.isEqual(hmac(rawBody), provided);
    }

    private byte[] hmac(byte[] data) {
        try {
            Mac mac = Mac.getInstance(HMAC);
            mac.init(key);
            return mac.doFinal(data);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("HmacSHA256 unavailable", e);   // JDK guarantees this algorithm
        }
    }
}
