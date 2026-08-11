package io.rpe.detection;

import io.rpe.domain.PaymentEvent;

/**
 * Contract for a pluggable anomaly detection rule.
 *
 * The unified Lua gate executes atomically per event and returns a {@link LuaGateResult}
 * carrying raw computed state (velocity count, Welford stats, geo snapshot, dedup outcome).
 * Each {@code Detector} receives that result and applies rule-specific threshold logic
 * in Java against {@code @ConfigurationProperties}-bound config (ADR-14).
 *
 * <p><b>Caller contract:</b> {@code evaluate()} is called ONLY when
 * {@link LuaGateResult#dedupBlocked()} is {@code false}.
 *
 * <p><b>Thread-safety:</b> implementations MUST be stateless and thread-safe.
 * A single instance is shared across all per-account lane virtual threads.
 */
public interface Detector {

    /**
     * Stable, immutable rule identifier.
     *
     * Used verbatim in: {@code alert_id = UUIDv5(NAMESPACE, eventId + ":" + ruleName())}.
     *
     * NEVER change after first deployment. Changing it generates a different {@code alert_id}
     * for the same event, silently breaking {@code processed_alerts} dedup and replay safety.
     * Pin to a package-private constant; assert the string value in a unit test.
     */
    String ruleName();

    /**
     * Evaluates the Lua gate result for this event and returns a verdict.
     *
     * Called on the per-account lane virtual thread, after
     * {@code publishOn(laneScheduler)} has returned execution to the lane.
     *
     * <p>Required guards by rule:
     * <ul>
     *   <li>GeoDetector: {@code if (result.geoPrev() == null) return CLEAN} — one null check
     *       covers all three co-dependent geo fields (ADR-14)
     *   <li>ZScoreDetector: {@code if (stddev == 0.0) return CLEAN} — prevents divide-by-zero
     * </ul>
     *
     * @param result          Lua gate output for this event
     * @param event           originating payment event (amount, lat/lon, payload timestamp)
     * @param brokerIngestMs  Kafka broker ingest timestamp from {@code ConsumerRecord.timestamp()};
     *                        used by GeoDetector for time-delta math (payload clocks drift)
     * @return {@link DetectionResult#clean} when rule does not fire;
     *         {@link DetectionResult#alert} with non-null ruleName when it does
     */
    DetectionResult evaluate(LuaGateResult result, PaymentEvent event, long brokerIngestMs);
}
