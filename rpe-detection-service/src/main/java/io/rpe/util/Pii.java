package io.rpe.util;

/**
 * PII redaction helpers — the single implementation for all log-line masking.
 *
 * Security rule: redact {@code account_id} to last 4 chars in any log line; no PII in
 * exception messages. Per-class copies of these helpers drift; every caller must use
 * this class.
 */
public final class Pii {

    private Pii() {}

    /** Last 4 chars only. Callers prefix with "..." in the log pattern. */
    public static String maskAccountId(String accountId) {
        if (accountId == null || accountId.length() < 4) return "****";
        return accountId.substring(accountId.length() - 4);
    }

    /** Last 8 chars only — enough to correlate, not enough to replay. */
    public static String maskEventId(String eventId) {
        if (eventId == null || eventId.length() < 8) return "****";
        return eventId.substring(eventId.length() - 8);
    }
}
