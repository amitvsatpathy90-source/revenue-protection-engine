#!/usr/bin/env python3
"""
RPE Synthetic E2E — fixture generator.

Produces one JSONL file per test cohort under ./events/. Each line is
"{accountId}:{PaymentEvent JSON}" — the format kafka-console-producer expects
with `--property parse.key=true --property key.separator=:` (split on the
FIRST colon only, so a JSON value containing colons is safe).

Grounded against:
  rpe-detection-service/src/main/java/io/rpe/domain/PaymentEvent.java   (schema)
  rpe-detection-service/src/main/java/io/rpe/detection/VelocityDetector.java (>=100/60s)
  rpe-detection-service/src/main/java/io/rpe/detection/ZScoreDetector.java  (>=30 samples, z>3.0)
  rpe-detection-service/src/main/resources/application.yml               (thresholds)

Re-run to regenerate; nothing here is hand-typed/error-prone repetition.
"""
import json
import random
from pathlib import Path

random.seed(42)  # deterministic fixtures — re-runs are reproducible, not re-randomized

OUT = Path(__file__).parent / "events"
OUT.mkdir(exist_ok=True)

BASE_TS = "2026-09-13T12:00:00Z"
SF = (37.7749, -122.4194)


def event(event_id, account_id, amount, lat, lon, ts=BASE_TS):
    return {
        "eventId": event_id,
        "accountId": account_id,
        "amount": round(amount, 2),
        "lat": lat,
        "lon": lon,
        "timestamp": ts,
        "schemaVersion": "1",
    }


def line(account_id, ev):
    return f"{account_id}:{json.dumps(ev)}"


def jitter(base=50.0, spread=5.0):
    # Non-zero variance is required — ZScoreDetector returns clean on stddev==0.
    return base + random.uniform(-spread, spread)


# ---------------------------------------------------------------------------
# 1. Clean baseline — 4 accounts x 3 events, no detector should fire.
# ---------------------------------------------------------------------------
lines = []
for i in range(1, 5):
    acct = f"acct-clean-{i:02d}"
    for j in range(1, 4):
        lines.append(line(acct, event(f"clean-{i:02d}-{j:02d}", acct, jitter(), *SF)))
Path(OUT / "01_clean.jsonl").write_text("\n".join(lines) + "\n")

# ---------------------------------------------------------------------------
# 2. Velocity boundary — 1 account x 101 events.
#    priorCount(N) = N-1. Event 100 -> priorCount=99 (clean). Event 101 -> priorCount=100 (ALERT).
# ---------------------------------------------------------------------------
acct = "acct-velocity-01"
lines = [line(acct, event(f"velocity-{n:03d}", acct, jitter(), *SF)) for n in range(1, 102)]
Path(OUT / "02_velocity_boundary.jsonl").write_text("\n".join(lines) + "\n")

# ---------------------------------------------------------------------------
# 3. Z-score — 1 account x 30 baseline (MIN_SAMPLES) + 1 outlier.
#    31 total events stays well under the velocity threshold (100/60s) — no cross-trigger.
# ---------------------------------------------------------------------------
acct = "acct-zscore-01"
lines = [line(acct, event(f"zscore-base-{n:02d}", acct, jitter(), *SF)) for n in range(1, 31)]
lines.append(line(acct, event("zscore-outlier-01", acct, 5000.00, *SF)))
Path(OUT / "03_zscore.jsonl").write_text("\n".join(lines) + "\n")

# ---------------------------------------------------------------------------
# 5. Precedence — 1 account x 100 filler (builds welfordCount>=30 AND velocityPriorCount
#    to exactly 100) + 1 event that is BOTH a velocity trip (priorCount=100) AND a z-score
#    outlier (amount=5000). PaymentEventConsumer breaks on first firing detector (@Order 10
#    before 20) — asserts alert.ruleName == "velocity", proving evaluation order, not just
#    declared order, decides the winner.
# ---------------------------------------------------------------------------
acct = "acct-precedence-01"
lines = [line(acct, event(f"precedence-filler-{n:03d}", acct, jitter(), *SF)) for n in range(1, 101)]
lines.append(line(acct, event("precedence-trigger-01", acct, 5000.00, *SF)))
Path(OUT / "05_precedence.jsonl").write_text("\n".join(lines) + "\n")

# ---------------------------------------------------------------------------
# 6a. Idempotency (gate-level) — reuses a z-score-style warm-up so the duplicated event is
#     itself alert-worthy (dedup's effect must be observable in the DB — a duplicated CLEAN
#     event proves nothing, since clean events never touch Postgres).
#     The outlier line is repeated twice with the IDENTICAL eventId — Redis
#     SET dedup:{eventId} NX EX 300 must absorb the second copy at the gate,
#     before any detector runs. Expect exactly 1 outbox/processed_alerts row.
# ---------------------------------------------------------------------------
acct = "acct-dedup-4a-01"
lines = [line(acct, event(f"dedup4a-base-{n:02d}", acct, jitter(), *SF)) for n in range(1, 31)]
dup_ev = event("dedup4a-outlier-01", acct, 5000.00, *SF)
lines.append(line(acct, dup_ev))
lines.append(line(acct, dup_ev))  # identical eventId, sent again within the 300s dedup TTL
Path(OUT / "06a_dedup_event.jsonl").write_text("\n".join(lines) + "\n")

print(f"Wrote fixtures to {OUT}/")
for f in sorted(OUT.glob("*.jsonl")):
    print(f"  {f.name}: {sum(1 for _ in f.open())} lines")
