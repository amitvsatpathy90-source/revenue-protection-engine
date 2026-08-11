package io.rpe.alert;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * RPE Alert Service (ADR-17 service #3) — idempotent alert actioning.
 *
 * Consumes {@code payment.alerts} (own group, {@code isolation.level=read_committed} —
 * required for transactional-producer compatibility) and records each alert in
 * {@code processed_alerts} via {@code INSERT … ON CONFLICT DO NOTHING}. The deterministic
 * UUIDv5 {@code alert_id} (ADR-13) makes redelivery a no-op: the same alert is always safe
 * to receive twice. Structurally broken alerts route to {@code payment.alerts.DLT}.
 *
 * Sole writer of {@code processed_alerts} (ADR-17 §3.4); the triage service reads it
 * read-only. Runtime: virtual-thread consumer; JDBC on a dedicated platform-thread pool.
 * No Redis, no WebFlux. Independent deployability (microservices.md §4): boots and consumes
 * whatever is on {@code payment.alerts} with every other service absent.
 */
@SpringBootApplication
@EnableScheduling
public class AlertApplication {

    public static void main(String[] args) {
        SpringApplication.run(AlertApplication.class, args);
    }
}
