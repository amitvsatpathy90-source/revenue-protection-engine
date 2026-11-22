package io.rpe.ingest.testsupport;

/**
 * Webhook bodies shaped after the gateway's documented {@code payment.*} payloads (fields the service reads:
 * event, created_at, payment.entity.{id,amount,currency,notes}). Signatures for these exact bytes with secret
 * {@code whsec_test} were computed independently with Python's hmac module.
 */
public final class WebhookBodies {

    public static final String SECRET = "whsec_test";
    /**
     * Event-level created_at in every body below.
     */
    public static final long EVENT_EPOCH = 1_691_735_748L;

    public static final String NOTES_ACCOUNT = "{\"accountId\":\"acc-smoke-1\"}";
    public static final String NOTES_EMPTY_ARRAY = "[]";

    public static final String CAPTURED_SIG = "b481b0dea1dbbe866dcbc424a9a535bbe2e50c8f77ba0b6f4de1a96c46ee8ca7";
    public static final String CAPTURED_EMPTY_NOTES_SIG =
            "7db6bbb77e443f98da32ea4c808ca052da92a8bee647b97adbf4cfbf23671233";
    public static final String AUTHORIZED_SIG = "9eb7421e4858120a3a07d7828eb713f6e017a657b910c5fb22a98193d54175a9";

    private WebhookBodies() {
    }

    /**
     * @param notes raw JSON for the notes value, e.g. {@link #NOTES_ACCOUNT} or {@link #NOTES_EMPTY_ARRAY}
     */
    public static String payment(String event, String status, String notes) {
        return "{\"entity\":\"event\",\"account_id\":\"acc_BFQ7uQEaa7j2z7\",\"event\":\"" + event
                + "\",\"contains\":[\"payment\"],\"payload\":{\"payment\":{\"entity\":{\"id\":\"pay_DESlfW9H8K9uqM\","
                + "\"entity\":\"payment\",\"amount\":29900,\"currency\":\"INR\",\"status\":\"" + status
                + "\",\"order_id\":\"order_DESlLckIVRkHWj\",\"method\":\"card\",\"notes\":" + notes
                + ",\"created_at\":1691735700}}},\"created_at\":1691735748}";
    }

    public static String captured() {
        return payment("payment.captured", "captured", NOTES_ACCOUNT);
    }

    public static String capturedEmptyNotes() {
        return payment("payment.captured", "captured", NOTES_EMPTY_ARRAY);
    }

    public static String authorized() {
        return payment("payment.authorized", "authorized", NOTES_ACCOUNT);
    }
}
