-- ADR-25: persist W3C trace context on the outbox row so the trace survives the durable-queue
-- hop. Detection writes traceparent/tracestate from the active consume span; the relay restores
-- them as the remote parent when it publishes to payment.alerts (one continuous trace across the
-- async outbox gap — the seam the k8s OTel agent cannot stitch).
--
-- Additive + nullable (microservices.md §2): pre-existing PENDING rows and any producer running
-- without tracing simply carry NULL, and the relay starts a fresh trace for them. No backfill.
-- The relay's test-fixture copy (V1__outbox_contract.sql) mirrors these columns.
ALTER TABLE outbox ADD COLUMN traceparent TEXT;
ALTER TABLE outbox ADD COLUMN tracestate  TEXT;
