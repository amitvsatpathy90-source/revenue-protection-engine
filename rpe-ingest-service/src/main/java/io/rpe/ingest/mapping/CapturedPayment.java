package io.rpe.ingest.mapping;

/**
 * The fields of a {@code payment.captured} webhook that the mapper needs, already extracted from the payload.
 * Nullable fields mean "absent in the payload"; validation happens in {@link PaymentEventMapper}.
 *
 * @param paymentId              payment entity id ({@code pay_...}); becomes the outbound eventId
 * @param currency               ISO currency code of the payment
 * @param amountMinor            amount in the smallest currency sub-unit (paise for INR)
 * @param accountId              value of {@code notes.accountId}, or null
 * @param latNote                raw {@code notes.lat} (scenario override), or null
 * @param lonNote                raw {@code notes.lon} (scenario override), or null
 * @param eventCreatedAtEpochSec top-level event {@code created_at}, epoch seconds (delivery-age basis)
 */
public record CapturedPayment(
        String paymentId,
        String currency,
        long amountMinor,
        String accountId,
        String latNote,
        String lonNote,
        long eventCreatedAtEpochSec) {
}
