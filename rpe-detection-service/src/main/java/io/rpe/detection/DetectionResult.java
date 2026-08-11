package io.rpe.detection;

import java.util.Objects;

/**
 * Verdict returned by a {@link Detector} implementation.
 */
public record DetectionResult(Verdict verdict, String ruleName, String reason) {

    public enum Verdict { ALERT, CLEAN }

    public static DetectionResult clean(String reason) {
        return new DetectionResult(Verdict.CLEAN, null, reason);
    }

    /**
     * @param ruleName used verbatim in UUIDv5(NAMESPACE, eventId + ":" + ruleName).
     *                 Must never be null — null corrupts the alert_id, silently breaking
     *                 processed_alerts dedup (replay safety keystone).
     */
    public static DetectionResult alert(String ruleName, String reason) {
        Objects.requireNonNull(ruleName, "ruleName null corrupts UUIDv5 alert_id dedup");
        return new DetectionResult(Verdict.ALERT, ruleName, reason);
    }

    public boolean isAlert() { return verdict == Verdict.ALERT; }
}
