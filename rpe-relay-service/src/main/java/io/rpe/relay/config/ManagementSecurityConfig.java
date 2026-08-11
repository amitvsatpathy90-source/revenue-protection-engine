package io.rpe.relay.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtValidators;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;

/**
 * Zero-trust Actuator hardening for the relay (servlet surface, ADR-19). Identical policy to
 * the detection reactive chain, expressed in the {@code HttpSecurity} idiom. Per-service copy
 * by design (no shared rpe-common jar, microservices.md §1.4).
 *
 * <p>{@link ConditionalOnWebApplication}(SERVLET) is load-bearing: the integration test boots
 * with {@code webEnvironment = NONE} where {@code HttpSecurity} does not exist; gating keeps
 * these beans (and the fail-closed decoder) out of non-web contexts so the suite still loads.
 *
 * <p>NOTE (spring-boot-4.md): verify the Security 7.0 servlet lambda DSL + SimpleClientHttp
 * timeout overloads against the changelog before promoting.
 */
@Configuration
@EnableWebSecurity
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
public class ManagementSecurityConfig {

    @Bean
    SecurityFilterChain actuatorSecurity(HttpSecurity http) throws Exception {
        return http
            .authorizeHttpRequests(auth -> auth
                .requestMatchers(HttpMethod.GET,
                        "/actuator/health/liveness", "/actuator/health/readiness").permitAll()
                .requestMatchers(HttpMethod.GET, "/actuator/prometheus")
                        .hasAuthority("SCOPE_metrics:scrape")
                .requestMatchers("/actuator/**").authenticated()
                .anyRequest().denyAll())
            .oauth2ResourceServer(o -> o.jwt(Customizer.withDefaults()))
            .csrf(AbstractHttpConfigurer::disable)
            .httpBasic(AbstractHttpConfigurer::disable)
            .formLogin(AbstractHttpConfigurer::disable)
            .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
            .build();
    }

    /**
     * Validates signature (JWKS, timeout-bounded fetch) + issuer + audience. Audience binding
     * closes the confused-deputy hole. Fail-closed: no defaults for jwk-set-uri/issuer.
     */
    @Bean
    JwtDecoder actuatorJwtDecoder(
            @Value("${rpe.security.oauth2.jwk-set-uri}") String jwkSetUri,
            @Value("${rpe.security.oauth2.issuer}") String issuer,
            @Value("${rpe.security.oauth2.audience}") String audience,
            @Value("${rpe.security.oauth2.jwks-connect-timeout-ms:2000}") long connectMs,
            @Value("${rpe.security.oauth2.jwks-read-timeout-ms:2000}") long readMs) {

        SimpleClientHttpRequestFactory rf = new SimpleClientHttpRequestFactory();
        rf.setConnectTimeout(Duration.ofMillis(connectMs));
        rf.setReadTimeout(Duration.ofMillis(readMs));

        NimbusJwtDecoder decoder = NimbusJwtDecoder.withJwkSetUri(jwkSetUri)
                .restOperations(new RestTemplate(rf))
                .build();
        OAuth2TokenValidator<Jwt> withIssuer   = JwtValidators.createDefaultWithIssuer(issuer);
        OAuth2TokenValidator<Jwt> withAudience = new AudienceValidator(audience);
        decoder.setJwtValidator(new DelegatingOAuth2TokenValidator<>(withIssuer, withAudience));
        return decoder;
    }
}
