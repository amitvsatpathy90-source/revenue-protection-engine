package io.rpe.ingest.web;

import io.rpe.ingest.service.IngestResult;
import io.rpe.ingest.service.WebhookIngestService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;

/**
 * Gateway webhook endpoint. The body is bound as raw bytes: the signature covers the exact bytes received, so
 * the payload must not be parsed and re-serialized before verification.
 */
@RestController
class WebhookController {

    private final WebhookIngestService service;

    WebhookController(WebhookIngestService service) {
        this.service = service;
    }

    @PostMapping("/webhooks/razorpay")
    ResponseEntity<Void> receive(@RequestBody byte[] body,
                                 @RequestHeader(value = "X-Razorpay-Signature", required = false) String signature,
                                 @RequestHeader(value = "X-Razorpay-Event-Id", required = false) String eventId) {
        IngestResult result = service.ingest(body, signature, eventId, Instant.now());
        return switch (result) {
            case ACCEPTED, DUPLICATE -> ResponseEntity.ok().build();
            case UNAUTHORIZED -> ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
            case BAD_REQUEST -> ResponseEntity.badRequest().build();
        };
    }
}
