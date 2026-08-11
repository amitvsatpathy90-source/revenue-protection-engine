package io.rpe.alert.config;

import org.springframework.security.oauth2.core.OAuth2Error;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidatorResult;
import org.springframework.security.oauth2.jwt.Jwt;

import java.util.List;
import java.util.Objects;

/**
 * Rejects any JWT whose {@code aud} claim does not contain this service's audience — closes the
 * OAuth2 confused-deputy hole. Error message carries no token data. Package-private.
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
        List<String> aud = jwt.getAudience();
        return (aud != null && aud.contains(requiredAudience))
                ? OAuth2TokenValidatorResult.success()
                : OAuth2TokenValidatorResult.failure(INVALID_AUDIENCE);
    }
}
