package io.rpe.ingest.mapping;

import java.time.Duration;

/**
 * Result of mapping one captured payment. Exactly one of three terminal states.
 */
public sealed interface MappingOutcome {

    /**
     * Valid and fresh: to be published.
     */
    record Forward(OutboundPaymentEvent event) implements MappingOutcome {
    }

    /**
     * Invalid for detection; stored with a reason, never published.
     */
    record Rejected(String reason) implements MappingOutcome {
    }

    /**
     * Delivered later than the allowed age; stored, never published.
     */
    record Stale(Duration age) implements MappingOutcome {
    }
}
