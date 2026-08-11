-- Stage 6: detection owns outbox, so it owns the outbox purge (ADR-17 §3.4 one-writer rule).
-- processed_alerts purge moved to ProcessedAlertsPurge in rpe-alert-service.
-- purge_old_records() function removed — relay_role called it across two owned tables,
-- violating one-writer-per-table. Each owner now runs its own @Scheduled DELETE.
-- This migration file is retained to preserve Flyway's checksum history; it is a no-op
-- for existing clusters where the function was already created (DROP IF EXISTS is safe).
DROP FUNCTION IF EXISTS purge_old_records();
