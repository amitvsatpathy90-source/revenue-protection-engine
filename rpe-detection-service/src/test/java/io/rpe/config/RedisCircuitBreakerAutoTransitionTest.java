package io.rpe.config;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.io.ClassPathResource;

import java.time.Duration;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the one CB setting the C+D fallback (ADR-02) cannot function without.
 *
 * Once the {@code redis} breaker opens, the fallback buffers every event WITHOUT calling
 * the gate — so nothing ever attempts a permission on the breaker again. Resilience4j's
 * OPEN→HALF_OPEN transition is lazy by default: "the transition to HALF_OPEN only happens
 * if a call is made, even after waitDurationInOpenState is passed". Lazy transition +
 * traffic-stopping fallback = mutual wait: the breaker waits for a call, the fallback
 * waits for OPEN_TO_HALF_OPEN. Detection wedges permanently (buffer fills → PAUSED →
 * consumer paused → zero probes forever), and Redis recovering does not un-wedge it.
 * {@code automaticTransitionFromOpenToHalfOpenEnabled: true} breaks the cycle: the
 * OPEN_TO_HALF_OPEN event fires on schedule and triggers the drain.
 *
 * CbFallbackHandlerTest masks this in isolation because it forces
 * {@code transitionToHalfOpenState()} manually — nothing in production issues that call.
 */
class RedisCircuitBreakerAutoTransitionTest {

    @Test
    void redisBreakerHasAutomaticOpenToHalfOpenTransition() {
        var yaml = new YamlPropertiesFactoryBean();
        yaml.setResources(new ClassPathResource("application.yml"));
        Properties props = yaml.getObject();

        assertThat(props).isNotNull();
        assertThat(props.getProperty(
                "resilience4j.circuitbreaker.instances.redis.automaticTransitionFromOpenToHalfOpenEnabled"))
                .as("the fallback stops all traffic to the open breaker; without automatic "
                        + "OPEN→HALF_OPEN the lazy transition never fires and detection wedges "
                        + "permanently — even after Redis recovers")
                .isEqualTo("true");
    }

    /** Documents the mechanism the fix relies on: with the flag, HALF_OPEN is reached with
     *  ZERO calls against the breaker — exactly the traffic pattern the fallback produces. */
    @Test
    void automaticTransitionReachesHalfOpenWithZeroTraffic() {
        CircuitBreaker cb = CircuitBreaker.of("probe", CircuitBreakerConfig.custom()
                .waitDurationInOpenState(Duration.ofMillis(100))
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .build());

        cb.transitionToOpenState();

        Awaitility.await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.HALF_OPEN));
    }
}
