package io.rpe.observability;

import io.rpe.relay.PostgresNotifyRelayTrigger;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

/**
 * Exposes Postgres LISTEN/NOTIFY connection health to {@code /actuator/health}.
 *
 * Two distinct failure modes are surfaced:
 *   1. Connection loss: {@code listenerHealthy=false} (PostgresNotifyRelayTrigger detects it)
 *   2. "Connected but trigger not firing": {@code lastNotificationAt} is stale under write load.
 *      A long notification gap while the outbox is actively receiving inserts indicates
 *      the V4 trigger was dropped or the LISTEN channel is dead without a TCP-level error.
 *
 * {@code rpe.relay.listener.healthy} gauge (0/1) is also published to Prometheus for alerting.
 */
@Component
public class RelayListenerHealthIndicator implements HealthIndicator {

    private final PostgresNotifyRelayTrigger relayTrigger;
    private final MeterRegistry             meterRegistry;

    public RelayListenerHealthIndicator(
            PostgresNotifyRelayTrigger relayTrigger, MeterRegistry meterRegistry) {
        this.relayTrigger  = relayTrigger;
        this.meterRegistry = meterRegistry;
    }

    @PostConstruct
    public void registerGauge() {
        Gauge.builder("rpe.relay.listener.healthy",
                        relayTrigger, t -> t.isListenerHealthy() ? 1.0 : 0.0)
                .description("1 if LISTEN/NOTIFY connection is healthy, 0 if not")
                .register(meterRegistry);
    }

    @Override
    public Health health() {
        if (!relayTrigger.isListenerHealthy()) {
            return Health.down()
                    .withDetail("reason", "LISTEN/NOTIFY connection is not established")
                    .build();
        }

        Instant lastNotify = relayTrigger.getLastNotificationAt();
        Health.Builder builder = Health.up();

        if (lastNotify != null) {
            long secondsSince = Duration.between(lastNotify, Instant.now()).toSeconds();
            builder.withDetail("lastNotificationAt", lastNotify.toString())
                   .withDetail("secondsSinceLastNotification", secondsSince);
        } else {
            builder.withDetail("lastNotificationAt", "never");
        }

        return builder.build();
    }
}
