#!/usr/bin/env bash
set -euo pipefail

RUNS="${1:-3}"

if ! [[ "$RUNS" =~ ^[0-9]+$ ]] || [[ "$RUNS" -lt 1 ]]; then
  echo "usage: $0 [runs>=1]" >&2
  exit 2
fi

for i in $(seq 1 "$RUNS"); do
  echo "RUN $i/$RUNS"
  if ! go run ./cmd/mtrrun engine_iuds >/tmp/mylite-engine-iuds-last.log 2>&1; then
    echo "engine_iuds failed on run $i" >&2
    tail -20 /tmp/mylite-engine-iuds-last.log >&2 || true
    exit 1
  fi
  tail -3 /tmp/mylite-engine-iuds-last.log
done

echo "engine_iuds passed $RUNS/$RUNS runs"
