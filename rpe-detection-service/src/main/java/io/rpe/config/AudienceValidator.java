package io.rpe.config;

import org.springframework.security.oauth2.core.OAuth2Error;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidatorResult;
import org.springframework.security.oauth2.jwt.Jwt;

import java.util.List;
import java.util.Objects;

/**
 * Rejects any JWT whose {@code aud} claim does not contain this relying party's audience.
 * Without it, ANY signature-valid token from the trusted issuer (e.g. one minted for a
 * different service) would be accepted here — the classic OAuth2 "confused deputy".
 *
 * <p>The error message carries NO token data (no PII / no secret) — only the static reason.
 * Package-private: constructed solely by {@code ManagementSecurityConfig} (boundary isolation).
 */
final class AudienceValidator implements OAuth2TokenValidator<Jwt> {

    private static final OAuth2Error INVALID_AUDIENCE =
            new OAuth2Error("invalid_token", "Required audience not present in token", null);

    private final String requiredAudience;

    AudienceValidator(String requiredAudience) {
        this.requiredAudience = Objects.requireNonNull(requiredAudience, "audience");
        if (requiredAudience.isBlank()) {
            throw new IllegalArgumentException("audience must not be blank");
        }
    }

    @Override
    public OAuth2TokenValidatorResult validate(Jwt jwt) {
        List<String> aud = jwt.getAudience(); // null when the claim is absent
        return (aud != null && aud.contains(requiredAudience))
                ? OAuth2TokenValidatorResult.success()
                : OAuth2TokenValidatorResult.failure(INVALID_AUDIENCE);
    }
}
