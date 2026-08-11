#!/usr/bin/env bash
set -euo pipefail

# check-path-drift.sh — CI gate: verifies Markdown/ADR cross-references point to real files.

# Catches explicit .md paths OR extensionless doc paths (e.g., docs/adrs/spring-boot-4)
EXTRACT_RE='(\./|\.\./|docs/)[A-Za-z0-9_./-]*(\.md)?'

echo "check-path-drift: scanning git-tracked documentation for dangling markdown references..."

misses=""
repo_root=$(pwd -P)

# Resolves ref (./ or ../) against base_dir into a repo-root-relative path.
# Pure bash cd+pwd — no realpath dependency, portable across BSD/GNU.
resolve_target() {
  local ref="$1" base_dir="$2"
  local ref_dir ref_base abs_dir
  ref_dir=$(dirname -- "$ref")
  ref_base=$(basename -- "$ref")
  abs_dir=$(cd "$base_dir/$ref_dir" 2>/dev/null && pwd -P) || { printf '%s\n' "$ref"; return; }
  case "$abs_dir/" in
    "$repo_root"/*) printf '%s\n' "${abs_dir#$repo_root/}/$ref_base" ;;
    *) printf '%s\n' "__OUT_OF_REPO__" ;;  # sibling/parent repo — out of scope for this gate
  esac
}

grep_output=$(git grep -I -n -E "$EXTRACT_RE" -- '*.md') && grep_rc=0 || grep_rc=$?
if [ "${grep_rc:-0}" -gt 1 ]; then
  echo "check-path-drift: git grep failed (exit $grep_rc) — aborting."
  exit "$grep_rc"
fi

while IFS=: read -r source_file line_num ref_line; do
  [ -z "${source_file:-}" ] && continue

  stripped_line=$(sed -E 's#https?://[^[:space:]]*##g' <<< "$ref_line")

  while IFS= read -r clean_path; do
    [ -z "$clean_path" ] && continue

    # Ignore raw directory paths or non-doc prefix tokens
    if [[ "$clean_path" =~ ^[a-zA-Z0-9_-]+/$ ]]; then continue; fi

    # Strip trailing sentence/markup punctuation (incl. trailing '.')
    clean_path=$(sed -E 's/[>,'"'"'"\)\],;.]*$//' <<< "$clean_path")

    if [[ "$clean_path" == ./* ]] || [[ "$clean_path" == ../* ]]; then
      source_dir=$(dirname "$source_file")
      target_path=$(resolve_target "$clean_path" "$source_dir")
      [ "$target_path" = "__OUT_OF_REPO__" ] && continue
    else
      target_path="$clean_path"
    fi

    # Assert disk existence directly OR with fallback .md extension
    if [ ! -e "$target_path" ] && [ ! -e "${target_path}.md" ]; then
      # Truncated match (e.g. brace-list shorthand ../rpe-{a,b}) — not a real path, skip
      [[ "$clean_path" =~ [-/]$ ]] && continue
      misses+="  dangling doc reference: $source_file:$line_num -> $clean_path"$'\n'
    fi
  done < <(grep -oE "$EXTRACT_RE" <<< "$stripped_line" || true)
done <<< "$grep_output"

if [ -n "$misses" ]; then
  echo "DOC PATH DRIFT DETECTED — broken document links found:"
  printf "%s" "$misses"
  exit 1
fi

echo "check-path-drift: OK — all referenced documentation files exist."