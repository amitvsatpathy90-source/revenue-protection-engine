package io.rpe.ingest.config;

import io.rpe.ingest.inbox.InboxRepository;
import io.rpe.ingest.mapping.PaymentEventMapper;
import io.rpe.ingest.mapping.RazorpayWebhookParser;
import io.rpe.ingest.security.WebhookSignatureVerifier;
import io.rpe.ingest.service.WebhookIngestService;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * Wires the verifier and mapper from {@link IngestProperties}; invalid configuration fails startup.
 */
@Configuration
@EnableConfigurationProperties(IngestProperties.class)
class IngestConfig {

    /**
     * A null or blank secret makes the verifier constructor throw, so the context fails to start.
     */
    @Bean
    WebhookSignatureVerifier webhookSignatureVerifier(IngestProperties props) {
        String secret = props.razorpay() == null ? null : props.razorpay().webhookSecret();
        return new WebhookSignatureVerifier(secret);
    }

    @Bean
    PaymentEventMapper paymentEventMapper(IngestProperties props) {
        List<PaymentEventMapper.GeoPoint> homes = props.homeLocations() == null
                ? List.of()
                : props.homeLocations().stream()
                .map(h -> new PaymentEventMapper.GeoPoint(h.lat(), h.lon()))
                .toList();
        return new PaymentEventMapper(
                new PaymentEventMapper.Config(props.maxEventAge(), props.maxFutureSkew(), homes, props.currency()));
    }

    @Bean
    RazorpayWebhookParser razorpayWebhookParser() {
        return new RazorpayWebhookParser();
    }

    @Bean
    WebhookIngestService webhookIngestService(WebhookSignatureVerifier verifier,
                                              RazorpayWebhookParser parser,
                                              PaymentEventMapper mapper,
                                              InboxRepository inbox) {
        return new WebhookIngestService(verifier, parser, mapper, inbox);
    }
}

