package io.rpe.triage;

import io.rpe.triage.config.TriageProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * rpe-triage-agent — advisory AI triage downstream of {@code payment.alerts} (ADR-15).
 *
 * Boundary (immutable): the LLM never gates detection, alert identity, or delivery.
 * This module consumes {@code payment.alerts}, produces {@code payment.alerts.triaged},
 * and writes only {@code triaged_alerts}. Agent down ⇒ alerts still flow.
 *
 * Runtime: single MVC + virtual-threads surface — deliberately NOT the core module's
 * dual WebFlux/VT split. The LLM call is blocking HTTP on a VT, bounded by the
 * Resilience4j stack. No WebFlux here (ADR-15 §3.9).
 */
@SpringBootApplication
@EnableConfigurationProperties(TriageProperties.class)
@EnableScheduling
public class TriageApplication {

    public static void main(String[] args) {
        SpringApplication.run(TriageApplication.class, args);
    }
}
