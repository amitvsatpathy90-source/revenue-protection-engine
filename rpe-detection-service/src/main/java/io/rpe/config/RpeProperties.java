package io.rpe.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "rpe")
public record RpeProperties(
        ResilienceProperties resilience,
        RateLimitingProperties rateLimiting,
        DetectionProperties detection) {

    // RelayProperties moved to rpe-relay-service (ADR-17 §7 Stage 1). Core is detection +
    // alert-actioning; it writes the outbox but no longer owns relay tuning.
    //
    // The rpe.dev.* flags (blockhound, reactor-debug-agent) are deliberately NOT bound here —
    // DevConfig gates them directly via @ConditionalOnProperty; a parallel DevProperties
    // binding was dead code that misread as the live toggle surface.

    public record ResilienceProperties(RedisResilienceProperties redis) {}

    public record RedisResilienceProperties(int bufferSize) {}

    public record RateLimitingProperties(
            int defaultLimitPerSec,
            int elevatedLimitPerSec,
            // ADR-24: distributed token-bucket params.
            // keyTtlMs — idle ratelimit:{account} eviction (bounds memory under account churn);
            //            eviction just resets a full bucket, so it is safe.
            // acquireTimeoutMs — the throttle-not-drop budget (was hardcoded 5s in ADR-03).
            long keyTtlMs,
            long acquireTimeoutMs) {}

    public record DetectionProperties(
            VelocityProperties velocity,
            ZScoreProperties zScore,
            GeoProperties geo,
            WelfordProperties welford,
            DedupProperties dedup,
            StatsProperties stats) {}

    public record VelocityProperties(
            long windowMs,
            int maxEvents,
            long velTtlMs) {}

    public record ZScoreProperties(double threshold) {}

    public record GeoProperties(double maxSpeedKmh, long geoTtlMs) {}

    public record WelfordProperties(int sampleCap) {}

    /** Per-event dedup key TTL. Externalised like the welford cap (ADR-08 precedent); the
     *  dedup-last + NX invariant (lua-gate.md) is unchanged — only the window is configurable. */
    public record DedupProperties(int ttlSeconds) {}

    /** Sliding idle TTL on {@code stats:{account}} Welford keys (ADR-27 — supersedes ADR-10's
     *  no-TTL sub-decision). Refreshed by every gate execution, so a hot account never expires;
     *  an account idle past the TTL is reclaimed instead of accumulating unevictably (no-TTL
     *  keys are invisible to volatile-lru — under churn they squeeze the dedup/vel/geo working
     *  set and terminally wedge Redis in noeviction write errors). Loss on expiry/eviction is
     *  bounded and self-healing: the account re-enters the &lt;30-sample z-score gate (same
     *  failure class as ADR-09 replay drift). */
    public record StatsProperties(long ttlMs) {}
}
