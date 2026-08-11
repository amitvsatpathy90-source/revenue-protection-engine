package io.rpe.relay;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Relay tuning, bound from {@code rpe.relay.*}.
 *
 * Extracted from the core module's {@code RpeProperties.RelayProperties} during the
 * ADR-17 §7 Stage 1 service split — the relay now owns its own config slice. The
 * property prefix is unchanged so existing config/env carries over.
 *
 * @param minPollMs   tight poll interval when rows were just processed
 * @param maxPollMs   backoff ceiling when idle (adaptive polling, ADR-05)
 * @param maxAttempts bounded attempts before a row is dead-lettered to FAILED
 */
@ConfigurationProperties(prefix = "rpe.relay")
public record RelayProperties(
        long minPollMs,
        long maxPollMs,
        int maxAttempts) {}
