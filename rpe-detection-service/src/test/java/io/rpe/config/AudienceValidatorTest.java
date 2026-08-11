package io.rpe.config;

import org.junit.jupiter.api.Test;
import org.springframework.security.oauth2.core.OAuth2TokenValidatorResult;
import org.springframework.security.oauth2.jwt.Jwt;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit coverage for the one piece of custom security logic on the Actuator boundary (ADR-19).
 * Runs with zero infrastructure — the authorization-rule wiring (scope/path) is covered
 * separately by a full {@code @SpringBootTest(RANDOM_PORT)} security test (follow-up).
 */
class AudienceValidatorTest {

    private final AudienceValidator validator = new AudienceValidator("rpe-actuator");

    private static Jwt jwt(List<String> audience) {
        Jwt.Builder b = Jwt.withTokenValue("token")
                .header("alg", "RS256")
                .subject("prometheus")
                .issuedAt(Instant.now())
                .expiresAt(Instant.now().plusSeconds(300));
        if (audience != null) {
            b.audience(audience);
        }
        return b.build();
    }

    @Test
    void acceptsWhenRequiredAudiencePresent() {
        assertThat(validator.validate(jwt(List.of("other", "rpe-actuator"))).hasErrors()).isFalse();
    }

    @Test
    void rejectsWhenAudienceIsForAnotherRelyingParty() {
        OAuth2TokenValidatorResult r = validator.validate(jwt(List.of("rpe-some-other-api")));
        assertThat(r.hasErrors()).isTrue();
        assertThat(r.getErrors()).anyMatch(e -> "invalid_token".equals(e.getErrorCode()));
    }

    @Test
    void rejectsWhenAudienceClaimAbsent() {
        assertThat(validator.validate(jwt(null)).hasErrors()).isTrue();
    }

    @Test
    void constructorRejectsBlankAudience() {
        assertThatThrownBy(() -> new AudienceValidator("  "))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
