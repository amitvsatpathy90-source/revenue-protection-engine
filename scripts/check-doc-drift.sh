#!/usr/bin/env bash
#
# check-doc-drift.sh — catches the AUTOMATABLE half of doc drift: a doc that references a test,
# class, or ADR that no longer exists in the code. This is the "dangling reference" failure mode —
# grep-shaped, so grep-checkable. It does NOT catch semantic drift (a claim that references a real
# file but describes behaviour the code doesn't have — e.g. the Apicurio serde case); that needs a
# human read, and the "How to read this file" preamble is the control for it.
#
# WHERE TO RUN IT:
#   - Locally, BEFORE uploading docs to the chat knowledge base. Locally the private files
# See operational documentation
#     drift actually originates.
# See operational documentation
#     checkout) — so CI catches README/design-doc drift, the local run catches everything.
#
# It is deliberately CONSERVATIVE — only two checks, both false-positive-free, because a drift check
# that cries wolf gets switched off. A dangling reference is the build's problem, not a warning.
#
# THE CLAIM-ANCHORING CONVENTION this enforces:
#   Every "proven by <X>" / "done" / gate-closure claim should name a real test/class/ADR. If it
#   names one, this script guarantees it still exists. If a claim can't name one, it isn't
#   verifiable — rewrite it so it can, or mark it explicitly as intent, not status.
#
set -uo pipefail
cd "$(dirname "$0")/.." || exit 2

misses=""

# Scan whatever doc files are present (private ones absent in CI — that's fine).
docs=()
for d in README.md ./*-design.md ; do
  [ -f "$d" ] && docs+=("$d")
done
if [ ${#docs[@]} -eq 0 ]; then echo "check-doc-drift: no doc files found"; exit 0; fi
echo "check-doc-drift: scanning ${docs[*]}"

# --- Check 1: every Test/IT class named in the docs exists as a .java file (test or main) ---
# The `Test`/`IT` suffix is ambiguous with English/SQL words (LIMIT, EXIT, INIT, AUDIT all end in
# "IT") and with framework annotations (@SpringBootTest et al. aren't repo classes). Exclude both so
# the check only flags a genuinely-dangling reference to one of THIS repo's own test classes.
frameworks="SpringBootTest DataJpaTest WebMvcTest WebFluxTest JsonTest RestClientTest JdbcTest DataRedisTest ParameterizedTest RepeatedTest"
for t in $(grep -rhoE "\b[A-Z][A-Za-z0-9]+(Test|IT)\b" "${docs[@]}" 2>/dev/null | sort -u); do
  case "$t" in ArchTest | Test | IT) continue ;; esac
  [[ " $frameworks " == *" $t "* ]] && continue          # framework annotation, not a repo class
  [[ "$t" =~ ^[A-Z0-9]+$ ]] && continue                  # all-caps word (LIMIT/EXIT/INIT), not a class
  if ! find . -path ./build -prune -o -path ./target -prune -o -name "${t}.java" -print 2>/dev/null | grep -q .; then
    misses+="  dangling test/class reference: ${t} (named in docs, no ${t}.java in tree)"$'\n'
  fi
done

# --- Check 2: every ADR-NNN referenced in the docs has a file in docs/adrs/ ---
for a in $(grep -rhoE "ADR-[0-9]+" "${docs[@]}" 2>/dev/null | sort -u); do
  if ! ls "docs/adrs/${a}.md" >/dev/null 2>&1; then
    misses+="  dangling ADR reference: ${a} (referenced in docs, no docs/adrs/${a}.md)"$'\n'
  fi
done

if [ -n "$misses" ]; then
  echo "DOC DRIFT DETECTED — a doc references something that no longer exists:"
  printf "%s" "$misses"
  echo "Fix the reference (or the code), then re-run."
  exit 1
fi
echo "check-doc-drift: OK — every referenced test/class/ADR exists."
