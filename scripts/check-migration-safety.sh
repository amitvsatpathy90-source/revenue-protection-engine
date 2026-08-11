#!/usr/bin/env bash
# See operational documentation
# schema-rules.md chaosforge, arch-audit F-09): a Flyway migration added in this PR must be
# safe to run while the PREVIOUS app version is still serving traffic (migrations run
# in-process at app boot on a rolling deploy — see the rule file for the full rationale).
# This script is the enforcement half; the rules file alone is advisory text an automated AI
# session reads, not a gate — it has no effect on a human-written migration or on CI.
#
# Scope: only NEWLY ADDED migration files in this diff. A modified EXISTING migration is a
# different, already-covered problem — Flyway's own checksum validation fails hard on that
# at apply time; this script does not re-check it.
#
# Usage: scripts/check-migration-safety.sh <migration-dir> [<migration-dir> ...]
#
# Requires enough git history to diff against the base branch — the calling workflow's
# checkout step needs fetch-depth: 0 (or at least a fetch of BASE_REF); a default shallow
# clone (actions/checkout@v4's default) makes BASE_REF resolution fail loudly rather than
# silently pass with zero files checked.
#
# Override: a migration that must ship a destructive change anyway (e.g. the "one release
# after the last reader is gone" case the rule describes) acknowledges it explicitly:
#   -- migration-safety: override <short reason>
# anywhere in the file. Every override is printed to the CI log — visible, not silent.

set -euo pipefail

BASE_REF="${BASE_REF:-origin/main}"

if [ "$#" -eq 0 ]; then
  echo "usage: $0 <migration-dir> [<migration-dir> ...]" >&2
  exit 2
fi

if ! git rev-parse --verify "${BASE_REF}" >/dev/null 2>&1; then
  echo "ERROR: '${BASE_REF}' not resolvable — checkout step needs fetch-depth: 0 (or fetch ${BASE_REF} explicitly)." >&2
  exit 2
fi

# name-status: 'A' == added in this diff. Modified/renamed migrations are out of scope (see header).
mapfile -t added_files < <(
  git diff --name-status --diff-filter=A "${BASE_REF}...HEAD" -- "$@" \
    | awk '{print $2}' \
    | grep -E '/V[0-9].*\.sql$' || true
)

if [ "${#added_files[@]}" -eq 0 ]; then
  echo "No newly added Flyway migrations in this diff — nothing to check."
  exit 0
fi

# Pattern -> human label. Matched case-insensitively, one grep -nE pass per file per pattern.
declare -A PATTERNS=(
  ["DROP[[:space:]]+COLUMN"]="DROP COLUMN"
  ["DROP[[:space:]]+TABLE"]="DROP TABLE"
  ["RENAME[[:space:]]+(COLUMN|TO)"]="RENAME"
  ["ALTER[[:space:]]+COLUMN[[:space:]]+[^ ]+[[:space:]]+SET[[:space:]]+NOT[[:space:]]+NULL"]="SET NOT NULL on existing column"
)

fail=0

for file in "${added_files[@]}"; do
  [ -f "$file" ] || continue   # deleted-then-added-elsewhere edge case; skip if gone from working tree

  if grep -qE '^\s*--\s*migration-safety:\s*override' "$file"; then
    reason=$(grep -E '^\s*--\s*migration-safety:\s*override' "$file" | head -1)
    echo "ACKNOWLEDGED: $file overrides the migration-safety check — ${reason}"
    continue
  fi

  file_hit=0
  for pattern in "${!PATTERNS[@]}"; do
    label="${PATTERNS[$pattern]}"
    while IFS=: read -r lineno content; do
      echo "VIOLATION: $file:$lineno — ${label} (destructive DDL against a shared table on a rolling deploy)"
      echo "    ${content}"
      file_hit=1
    done < <(grep -inE "${pattern}" "$file" || true)
  done

  if [ "$file_hit" -eq 1 ]; then
    fail=1
  fi
done

if [ "$fail" -eq 1 ]; then
  echo
  echo "One or more newly added migrations contain destructive DDL with no override marker."
  echo "Either split into expand/contract steps, or add:"
  echo "  -- migration-safety: override <short reason>"
  echo "to the migration if the destructive change genuinely ships this release."
  exit 1
fi

echo "All ${#added_files[@]} newly added migration(s) pass the migration-safety check."
