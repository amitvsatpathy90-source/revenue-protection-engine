package io.rpe.relay;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * RPE Relay Service (ADR-17 service #2) — exactly-once delivery.
 *
 * Reads PENDING rows from the {@code outbox} table (detection's published interface),
 * publishes each to {@code payment.alerts} with a Kafka-transactional producer, then
 * transitions the row to PUBLISHED. Sole writer of outbox ROWS is detection; this
 * service only transitions {@code status}/{@code attempts} (ADR-17 §3.4).
 *
 * Runtime surface (ADR-01, applied per service): a single background virtual thread
 * runs the poll loop; all JDBC runs on a dedicated platform-thread pool (PgJDBC
 * {@code synchronized} pins VT carriers). No WebFlux, no Redis, no Kafka consumer.
 *
 * Independent deployability (ADR-17 §3.5 / microservices.md §4): boots and drains the
 * existing outbox with detection absent. Cross-process wakeup is the V4 Postgres
 * {@code pg_notify('outbox_ready')} trigger (detection's in-process signal is gone —
 * "no-op when relay is extracted", ADR-05).
 */
// scanBasePackages = "io.rpe": the app class sits in io.rpe.relay, but the
// observability beans (TraceContextReader — ADR-25; RelayListenerHealthIndicator) live in the
// sibling io.rpe.observability. Default scan (the app's own package) would miss them —
// which had silently left RelayListenerHealthIndicator unregistered. Scan from the common root,
// matching detection's model. No cross-service code is on the relay classpath (microservices.md
// §1.4: no shared jar), so the wider scan only ever sees this service's own packages.
@SpringBootApplication(scanBasePackages = "io.rpe")
@EnableConfigurationProperties(RelayProperties.class)
public class RelayApplication {

    public static void main(String[] args) {
        SpringApplication.run(RelayApplication.class, args);
    }
}
