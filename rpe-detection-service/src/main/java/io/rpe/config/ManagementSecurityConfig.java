package io.rpe.config;

import io.netty.channel.ChannelOption;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtValidators;
import org.springframework.security.oauth2.jwt.NimbusReactiveJwtDecoder;
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoder;
import org.springframework.security.web.server.SecurityWebFilterChain;
import org.springframework.security.web.server.savedrequest.NoOpServerRequestCache;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

import java.time.Duration;

/**
 * Zero-trust hardening of the management (Actuator) surface — the ONLY HTTP surface this
 * service exposes (no business API; ingestion is Kafka-only, ADR-17/ADR-19). Network
 * reachability is not authorization: every Actuator call carries a validated JWT, except the
 * two k8s probes which are public but detail-free.
 *
 * <p>Per-service class by design — there is no shared rpe-common jar (microservices.md §1.4);
 * this is symmetric with the duplicated {@code Pii} helper. Servlet services (relay, alert,
 * triage) carry the {@code HttpSecurity} analog of this same policy.
 *
 * <p><b>{@link ConditionalOnWebApplication}(REACTIVE) is load-bearing:</b> the integration
 * tests boot with {@code webEnvironment = NONE}, where {@code ServerHttpSecurity} does not
 * exist. Gating on a reactive web application keeps these beans (and the fail-closed decoder)
 * out of non-web contexts so the test suite still loads.
 *
 * <p>NOTE (spring-boot-4.md): the reactive lambda DSL below is stable since Security 5.x
 * reactive and retained in Security 7.0. The {@code *Spec::disable} method-refs and
 * {@code oauth2ResourceServer} shape should be confirmed against the 7.0 changelog before
 * promoting — 7.0 removed several deprecated NON-lambda overloads (none used here).
 */
@Configuration
@EnableWebFluxSecurity
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.REACTIVE)
public class ManagementSecurityConfig {

    @Bean
    SecurityWebFilterChain actuatorSecurity(ServerHttpSecurity http) {
        return http
            .authorizeExchange(ex -> ex
                // Probes: public, detail-free (UP/DOWN). The kubelet cannot present a token;
                // these two groups expose no component/dependency data (show-details gated to
                // when-authorized in application.yml).
                .pathMatchers(HttpMethod.GET,
                        "/actuator/health/liveness",
                        "/actuator/health/readiness").permitAll()
                // Scrape: least-privilege read scope, not merely "authenticated".
                .pathMatchers(HttpMethod.GET, "/actuator/prometheus")
                        .hasAuthority("SCOPE_metrics:scrape")
                // Aggregate health, info: any authenticated principal.
                .pathMatchers("/actuator/**").authenticated()
                // Zero-trust default: no business HTTP surface exists here — refuse everything else.
                .anyExchange().denyAll())
            .oauth2ResourceServer(oauth2 -> oauth2.jwt(Customizer.withDefaults()))
            // Stateless bearer surface: no session, no CSRF token, no browser login/logout.
            .csrf(ServerHttpSecurity.CsrfSpec::disable)
            .httpBasic(ServerHttpSecurity.HttpBasicSpec::disable)
            .formLogin(ServerHttpSecurity.FormLoginSpec::disable)
            .logout(ServerHttpSecurity.LogoutSpec::disable)
            .requestCache(c -> c.requestCache(NoOpServerRequestCache.getInstance()))
            .build();
    }

    /**
     * Resource-server decoder: validates signature (JWKS) + issuer + expiry/nbf (default
     * validators) AND audience (custom). The audience check closes the confused-deputy hole —
     * a signature-valid token minted for a different relying party must not grant access here.
     *
     * <p>The JWKS fetch is the only outbound network call in the auth path; it runs through a
     * bounded-pool, timeout-capped WebClient (every network op gets connect/read timeouts and a
     * bounded pool).
     *
     * <p><b>Fail-closed:</b> {@code jwk-set-uri}/{@code issuer} have NO defaults. Missing env
     * aborts startup rather than booting an unauthenticated surface (same discipline as
     * {@code DB_PASSWORD}).
     */
    @Bean
    ReactiveJwtDecoder actuatorJwtDecoder(
            @Value("${rpe.security.oauth2.jwk-set-uri}") String jwkSetUri,
            @Value("${rpe.security.oauth2.issuer}") String issuer,
            @Value("${rpe.security.oauth2.audience}") String audience,
            @Value("${rpe.security.oauth2.jwks-connect-timeout-ms:2000}") long connectMs,
            @Value("${rpe.security.oauth2.jwks-read-timeout-ms:2000}") long readMs) {

        NimbusReactiveJwtDecoder decoder = NimbusReactiveJwtDecoder
                .withJwkSetUri(jwkSetUri)
                .webClient(boundedJwksClient(connectMs, readMs))
                .build();

        OAuth2TokenValidator<Jwt> withIssuer   = JwtValidators.createDefaultWithIssuer(issuer);
        OAuth2TokenValidator<Jwt> withAudience = new AudienceValidator(audience);
        decoder.setJwtValidator(new DelegatingOAuth2TokenValidator<>(withIssuer, withAudience));
        return decoder;
    }

    private WebClient boundedJwksClient(long connectMs, long readMs) {
        HttpClient httpClient = HttpClient.create(
                        ConnectionProvider.builder("jwks-pool")
                                .maxConnections(4)
                                .pendingAcquireTimeout(Duration.ofSeconds(2))
                                .build())
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) connectMs)
                .responseTimeout(Duration.ofMillis(readMs));
        return WebClient.builder()
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .build();
    }
}
