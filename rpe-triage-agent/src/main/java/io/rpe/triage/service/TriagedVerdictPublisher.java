package io.rpe.triage.service;

import io.rpe.triage.domain.TriagedAlertMessage;

/**
 * Publishes verdicts to {@code payment.alerts.triaged}.
 *
 * Interface so the replay/sweep tests can capture verdicts in memory
 * (Mockito unusable on this JVM). The ONLY topic this module writes —
 * boundary rule §1: never outbox, processed_alerts, payment.alerts,
 * or any core Redis key.
 */
public interface TriagedVerdictPublisher {

    /**
     * Sends and CONFIRMS the verdict. Returns only after broker ack — the caller
     * marks the inbox row triaged after this, so a crash mid-call leaves the row
     * PENDING_TRIAGE for the sweep (re-emit degraded) rather than silently lost.
     *
     * @throws PublishFailedException if the broker did not confirm
     */
    void publish(TriagedAlertMessage verdict);

    class PublishFailedException extends RuntimeException {
        public PublishFailedException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
