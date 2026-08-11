#!/usr/bin/env bash
#
# check-rules-sync.sh — verifies the two copies of every Prometheus rules file agree on their
# FUNCTIONAL fields (alert name, expr, for, labels).
#
# WHY THIS EXISTS: the canonical rules live in monitoring/rules/*.yml (mounted by both compose
# topologies), but the k8s topology carries an inline COPY inside the prometheus-config ConfigMap
# (deploy/k8s/observability/54-prometheus.yaml) — there is no kustomize/generation step, so the
# copies are kept in sync by hand. The 2026-07 arch audit (R6) found they had already drifted in
# their annotations; the next drift lands in expr/for and prod alerts silently diverge from the
# documented ones. This script is the mechanical guard dlt-operations.md's "both copies must stay
# in sync" rule relies on.
#
# Scope: functional fields only. Annotations/summaries/comments MAY differ (the k8s copy is
# deliberately terser); an expr/for/labels/alert-set difference is a FAILURE.
#
# WHERE TO RUN: locally after editing any monitoring/rules/*.yml or the ConfigMap copy — same
# discipline as check-doc-drift.sh. Requires ruby (macOS ships it); no gem installs.
#
# Exit codes: 0 = in sync; 1 = drift or a rules file missing from the ConfigMap.
#
set -euo pipefail
cd "$(dirname "$0")/.."

CANON_DIR="monitoring/rules"
K8S_FILE="deploy/k8s/observability/54-prometheus.yaml"

ruby -ryaml <<'RUBY'
canon_dir = "monitoring/rules"
k8s_file  = "deploy/k8s/observability/54-prometheus.yaml"

docs = YAML.load_stream(File.read(k8s_file))
cm = docs.find { |d| d && d["kind"] == "ConfigMap" && d.dig("data", "prometheus.yml") }
abort "FAIL: prometheus-config ConfigMap not found in #{k8s_file}" unless cm

functional = lambda do |yaml_text, origin|
  parsed = YAML.safe_load(yaml_text) or abort "FAIL: #{origin} did not parse"
  parsed["groups"].flat_map { |g| g["rules"] }
        .map { |r| { alert: r["alert"], expr: r["expr"], for: r["for"], labels: r["labels"] } }
end

failed = false
Dir.glob("#{canon_dir}/*.yml").sort.each do |path|
  name = File.basename(path)
  copy = cm["data"][name]
  unless copy
    puts "FAIL  #{name}: present in #{canon_dir}/ but MISSING from the k8s ConfigMap"
    failed = true
    next
  end
  a = functional.call(File.read(path), path)
  b = functional.call(copy, "#{k8s_file}##{name}")
  if a == b
    puts "OK    #{name}: #{a.map { |r| r[:alert] }.join(", ")}"
  else
    puts "FAIL  #{name}: functional fields diverged between #{canon_dir}/ and the k8s ConfigMap"
    (a | b).each do |r|
      puts "        canonical-only: #{r.inspect}" if a.include?(r) && !b.include?(r)
      puts "        k8s-only:       #{r.inspect}" if b.include?(r) && !a.include?(r)
    end
    failed = true
  end
end

# Also require every ConfigMap rules key to exist canonically (a k8s-only rules file is drift too).
cm["data"].keys.grep(/\.rules\.yml$/).each do |name|
  unless File.exist?("#{canon_dir}/#{name}")
    puts "FAIL  #{name}: present in the k8s ConfigMap but MISSING from #{canon_dir}/"
    failed = true
  end
end

exit(failed ? 1 : 0)
RUBY
