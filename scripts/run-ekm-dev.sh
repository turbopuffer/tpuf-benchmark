#!/usr/bin/env bash
# Run the full benchmark battery against the gcp-ekm-dev cluster.
#
# All writes force mode:ekm encryption with the gcp-ekm-dev-key-1/2 keys
# served by the in-cluster mock EKM server (see pkg/bench/namespace.go).
#
# Requires TURBOPUFFER_API_KEY for an EKM-enabled org on the cluster. The
# EKM testing org keys are in the byoc-testing worktree:
#   byoc-testing-gcp-ekm-dev/gcp-ekm-dev/values.overrides.yaml
#
# Usage:
#   TURBOPUFFER_API_KEY=... ./scripts/run-ekm-dev.sh                # 10m hot/cold suites
#   TURBOPUFFER_API_KEY=... ./scripts/run-ekm-dev.sh benchmarks/vector-knn-1m-hot.toml ...
#
# Env knobs:
#   ENDPOINT     target API endpoint (default https://gcp-ekm-dev.turbopuffer.com)
#   DURATION     override each benchmark's duration, e.g. 5m
#   IF_NONEMPTY  behavior for non-empty namespaces: clear|skip-upsert|abort (default clear)
set -euo pipefail

cd "$(dirname "$0")/.."

: "${TURBOPUFFER_API_KEY:?set TURBOPUFFER_API_KEY to an EKM org API key for gcp-ekm-dev}"

ENDPOINT="${ENDPOINT:-https://gcp-ekm-dev.turbopuffer.com}"
IF_NONEMPTY="${IF_NONEMPTY:-clear}"
DURATION="${DURATION:-}"

go build -o tpufbench ./cmd/tpufbench

# Default to the website 10m hot/cold suites. The top-level definitions
# (100m, 1b) ingest far too much data for the dev cluster.
if [ "$#" -gt 0 ]; then
  benchmarks=("$@")
else
  benchmarks=()
  while IFS= read -r line; do benchmarks+=("$line"); done < <(./tpufbench list ./benchmarks/website)
fi

extra_flags=()
if [ -n "$DURATION" ]; then
  extra_flags+=(--duration "$DURATION")
fi

echo "target: $ENDPOINT"
echo "benchmarks (${#benchmarks[@]}):"
printf '  %s\n' "${benchmarks[@]}"

failed=()
for bench in "${benchmarks[@]}"; do
  name="$(basename "$bench" .toml)"
  echo
  echo "==> $bench"
  if ! ./tpufbench run \
      --endpoint "$ENDPOINT" \
      --namespace-prefix "ekm_${name}" \
      --if-nonempty "$IF_NONEMPTY" \
      "${extra_flags[@]}" \
      "$bench"; then
    echo "==> FAILED: $bench"
    failed+=("$bench")
  fi
done

echo
if [ "${#failed[@]}" -gt 0 ]; then
  echo "${#failed[@]}/${#benchmarks[@]} benchmarks failed:"
  printf '  %s\n' "${failed[@]}"
  exit 1
fi
echo "all ${#benchmarks[@]} benchmarks passed"
