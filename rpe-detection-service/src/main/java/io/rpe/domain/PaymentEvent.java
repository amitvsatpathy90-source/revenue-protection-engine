package io.rpe.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.validation.constraints.*;
import java.math.BigDecimal;
import java.time.Instant;

/**
 * Inbound payment event DTO.
 *
 * Annotated with {@code @JsonIgnoreProperties(ignoreUnknown = true)} — removing this annotation
 * breaks additive schema evolution: a deploy with a new field crashes consumers deserializing
 * buffered events written under the old schema (ADR-11).
 *
 * {@code timestamp} is the payload clock — NOT used for time-delta calculations.
 * Payload clocks drift. Broker ingest time is obtained from {@code ConsumerRecord.timestamp()}.
 *
 * {@code schemaVersion} enables future versioned branching without field removal.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record PaymentEvent(
        @NotBlank  String      eventId,
        @NotBlank  String      accountId,
        @NotNull @DecimalMin("0.00") BigDecimal amount,
        @NotNull @DecimalMin("-90.0")  @DecimalMax("90.0")  BigDecimal lat,
        @NotNull @DecimalMin("-180.0") @DecimalMax("180.0") BigDecimal lon,
        @NotNull   Instant     timestamp,
        String schemaVersion
) {}
