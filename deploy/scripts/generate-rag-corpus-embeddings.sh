#!/usr/bin/env bash
# generate-rag-corpus-embeddings.sh — one-time operator tool (ADR-30).
# Generates pre-computed OpenAI embeddings for the hand-authored RAG corpus.
# Output is pasted into V3__seed_triage_rag_corpus.sql — NOT run automatically,
# NOT part of mvn verify or CI (same class of tool as deploy/kafka/dlt-redrive.sh).
#
# Usage:
#   export $(grep SPRING_AI_OPENAI_API_KEY .env | xargs)
#   ./deploy/scripts/generate-rag-corpus-embeddings.sh > corpus-insert.sql
#
# Requires: curl, jq, a real (non-sentinel) SPRING_AI_OPENAI_API_KEY.
set -euo pipefail

: "${SPRING_AI_OPENAI_API_KEY:?SPRING_AI_OPENAI_API_KEY not set}"

declare -A CORPUS=(
  ["velocity"]="Fires when the number of payment events for an account within a sliding time window exceeds a configured maximum. Counts prior events in the window before the current event is added, so the check is exclusive of the triggering event itself. A high-frequency burst of transactions on one account is the signal."
  ["zscore"]="Fires when a payment amount deviates from an account's historical spending pattern by more than a configured number of standard deviations, using an incrementally-updated running mean and variance (Welford's algorithm). Requires a minimum number of prior observations before evaluating, to avoid flagging early transactions on a new or sparse account as anomalous."
  ["geo"]="Fires when the implied travel speed between two consecutive payment locations for the same account exceeds a configured maximum, calculated from the great-circle distance between the two points and the elapsed time between them. Accounts with no prior location on record, or accounts explicitly classified as exempt from per-account geo ordering, are skipped rather than flagged."
)

echo "INSERT INTO triage_rag_corpus (rule_name, content, embedding) VALUES"
first=true
for rule in "${!CORPUS[@]}"; do
  content="${CORPUS[$rule]}"
  vector=$(curl -s https://api.openai.com/v1/embeddings \
    -H "Authorization: Bearer $SPRING_AI_OPENAI_API_KEY" \
    -H "Content-Type: application/json" \
    -d "$(jq -n --arg input "$content" '{input: $input, model: "text-embedding-3-small"}')" \
    | jq -c '.data[0].embedding')

  [ "$first" = true ] && first=false || echo ","
  printf "('%s', '%s', '%s'::vector)" "$rule" "$(echo "$content" | sed "s/'/''/g")" "$vector"
done
echo ";"