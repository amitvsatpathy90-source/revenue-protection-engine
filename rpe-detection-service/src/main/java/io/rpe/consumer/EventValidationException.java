package io.rpe.consumer;

/**
 * A semantically invalid {@code PaymentEvent} — Bean Validation constraint violations at
 * the ingestion boundary. Thrown on the listener thread BEFORE lane submission so it
 * propagates to {@code DefaultErrorHandler}.
 *
 * Registered not-retryable: validation failures are deterministic per event; retrying
 * burns the retry budget and delays DLT routing with no chance of a different outcome.
 *
 * The message carries violation COUNT only — field names and values stay out of
 * exception messages (PII rule: getMessage() outlives the log sink).
 */
public class EventValidationException extends RuntimeException {

    public EventValidationException(int violationCount) {
        super("PaymentEvent constraint violation(s): " + violationCount + " field(s) invalid");
    }
}
