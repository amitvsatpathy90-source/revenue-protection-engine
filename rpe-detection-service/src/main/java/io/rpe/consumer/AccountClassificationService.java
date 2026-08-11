package io.rpe.consumer;

import io.rpe.util.Pii;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.scheduler.Scheduler;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Classifies accounts as elevated or geo-exempt using Redis SETs, backed by a 60-second
 * Caffeine cache. The cache prevents a per-event Redis round-trip for classification.
 *
 * Redis keys are refreshed without a redeploy — cache TTL (60s) is the propagation lag.
 * Caffeine provides a 60-second resilience window if Redis is unavailable.
 *
 * Geo exemption is never silent: the first lookup that classifies an account as exempt
 * logs it (masked id), and {@code GeoDetector} increments a bounded skip counter (ADR-04).
 */
@Service
public class AccountClassificationService {

    private static final Logger log = LoggerFactory.getLogger(AccountClassificationService.class);

    private static final String ELEVATED_KEY   = "rpe:config:elevated_accounts";
    private static final String GEO_EXEMPT_KEY = "rpe:config:geo_exempt_accounts";

    /** Bounded wait for a classification lookup — a hung Redis must not wedge the lane. */
    private static final Duration LOOKUP_TIMEOUT = Duration.ofSeconds(2);

    private final ReactiveStringRedisTemplate redis;
    private final Scheduler laneScheduler;
    private final Cache<String, Boolean> elevatedCache;
    private final Cache<String, Boolean> geoExemptCache;

    public AccountClassificationService(ReactiveStringRedisTemplate redis, Scheduler laneScheduler) {
        this.redis = redis;
        this.laneScheduler = laneScheduler;
        this.elevatedCache = Caffeine.newBuilder()
                .maximumSize(50_000)
                .expireAfterWrite(60, TimeUnit.SECONDS)
                .build();
        this.geoExemptCache = Caffeine.newBuilder()
                .maximumSize(50_000)
                .expireAfterWrite(60, TimeUnit.SECONDS)
                .build();
    }

    public boolean isElevated(String accountId) {
        return classify(elevatedCache, ELEVATED_KEY, accountId, "ELEVATED (rate limit raised)");
    }

    public boolean isGeoExempt(String accountId) {
        // Explicit opt-in must be observable — geo bypass is never silent (ADR-04)
        return classify(geoExemptCache, GEO_EXEMPT_KEY, accountId, "GEO-EXEMPT (geo rule bypassed)");
    }

    private boolean classify(Cache<String, Boolean> cache, String key, String accountId, String logVerb) {
        Boolean cached = cache.getIfPresent(accountId);
        if (cached != null) return cached;
        boolean result = querySet(key, accountId);
        if (result) {
            log.info("Account ...{} classified as {}", Pii.maskAccountId(accountId), logVerb);
        }
        cache.put(accountId, result);
        return result;
    }

    private boolean querySet(String key, String accountId) {
        try {
            // publishOn(laneScheduler) after the reactive Redis op — mandatory even though there is
            // no downstream operator today (reactive-pipeline.md / System Invariants).
            // block() then parks the lane VT without pinning a carrier. The hop is the guardrail: a
            // later .map()/.filter() added here would otherwise run on the Lettuce I/O thread — the
            // silent-deadlock path — in a method called twice per hot-path event (arch-audit).
            return Boolean.TRUE.equals(
                    redis.opsForSet().isMember(key, accountId)
                            .publishOn(laneScheduler)
                            .block(LOOKUP_TIMEOUT));
        } catch (RuntimeException e) {
            // Redis unavailable — default to non-elevated/non-exempt.
            // .block() only throws unchecked (timeout/RedisException); RuntimeException is the
            // narrowest catch that preserves the fail-safe default (error-boundaries.md).
            // Caffeine TTL provides up to 60s of stale reads as resilience window.
            log.warn("Classification lookup failed for key {}, defaulting to false: {}",
                    key, e.getMessage());
            return false;
        }
    }
}
