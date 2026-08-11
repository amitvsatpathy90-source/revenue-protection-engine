package io.rpe;

import io.rpe.config.RpeProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Revenue Protection Engine — payment-event anomaly detection.
 *
 * Runtime surface split (ADR-01):
 *   HTTP / actuator : WebFlux (Netty) — non-blocking I/O
 *   Kafka consumer  : Virtual threads via @KafkaListener
 *   Redis hot path  : Reactive Lettuce (non-blocking, does not pin VT carriers)
 *   JDBC (outbox)   : Dedicated platform-thread pool (PgJDBC synchronized pins VTs)
 */
@SpringBootApplication
@EnableConfigurationProperties(RpeProperties.class)
@EnableScheduling
public class RpeApplication {

    public static void main(String[] args) {
        SpringApplication.run(RpeApplication.class, args);
    }
}
