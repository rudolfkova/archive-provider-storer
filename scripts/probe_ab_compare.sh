#!/usr/bin/env bash
# Compare baseline vs hedge probe outputs in an A/B directory.
# Usage: scripts/probe_ab_compare.sh timings/ab-<ts>
set -euo pipefail

AB_DIR="${1:?usage: $0 <ab-dir>}"
BASE_JSONL="$(ls -1 "$AB_DIR"/baseline/loop-probe-*.jsonl 2>/dev/null | tail -1 || true)"
HEDGE_JSONL="$(ls -1 "$AB_DIR"/hedge/loop-probe-*.jsonl 2>/dev/null | tail -1 || true)"
BASE_SUM="$(ls -1 "$AB_DIR"/baseline/summary-probe-*.json 2>/dev/null | tail -1 || true)"
HEDGE_SUM="$(ls -1 "$AB_DIR"/hedge/summary-probe-*.json 2>/dev/null | tail -1 || true)"

if [ -z "$BASE_SUM" ] || [ -z "$HEDGE_SUM" ]; then
  echo "missing summary files under $AB_DIR/{baseline,hedge}/" >&2
  exit 1
fi

wall_ms() {
  python3 - "$1" <<'PY'
import json, sys
from datetime import datetime
s = json.load(open(sys.argv[1]))
a = datetime.fromisoformat(s["loop_started_at"].replace("Z", "+00:00"))
b = datetime.fromisoformat(s["loop_finished_at"].replace("Z", "+00:00"))
print(int((b - a).total_seconds() * 1000))
PY
}

bucket() {
  local jsonl="$1"
  if [ -z "$jsonl" ] || [ ! -f "$jsonl" ]; then
    echo "n/a"
    return
  fi
  jq -r 'select(.op=="storage_rates_connect") | .latency_ms' "$jsonl" | LC_ALL=C awk '
    {
      n++; s+=$1
      if ($1 < 500) a++
      else if ($1 < 1500) b++
      else if ($1 < 4500) c++
      else if ($1 < 7500) d++
      else e++
    }
    END {
      if (n==0) { print "no samples"; exit }
      printf "<0.5s=%d  0.5-1.5s=%d  ~3s=%d  ~6s=%d  9-10s=%d  sum=%.1fs\n", a+0,b+0,c+0,d+0,e+0, s/1000
    }'
}

op_line() {
  local sum="$1" label="$2"
  python3 - "$sum" "$label" <<'PY'
import json, sys
s = json.load(open(sys.argv[1]))
label = sys.argv[2]
c = s["ops"].get("storage_rates_connect", {})
print(f"{label:8} avg={c.get('avg_ms','?'):>5}  p50={c.get('p50_ms','?'):>5}  p95={c.get('p95_ms','?'):>5}  max={c.get('max_ms','?'):>5}  err={c.get('errors','?')}  to={c.get('timeouts','?')}")
PY
}

BASE_WALL=$(wall_ms "$BASE_SUM")
HEDGE_WALL=$(wall_ms "$HEDGE_SUM")
DELTA=$((BASE_WALL - HEDGE_WALL))

REPORT="$AB_DIR/compare.txt"
{
  echo "A/B probe compare"
  echo "dir: $AB_DIR"
  echo "baseline summary: $BASE_SUM"
  echo "hedge summary:    $HEDGE_SUM"
  echo
  echo "wall_clock_ms  baseline=$BASE_WALL  hedge=$HEDGE_WALL  delta(baseline-hedge)=${DELTA}ms"
  echo
  echo "storage_rates_connect:"
  op_line "$BASE_SUM" "baseline"
  op_line "$HEDGE_SUM" "hedge"
  echo
  echo "connect latency buckets:"
  echo -n "baseline  "; bucket "$BASE_JSONL"
  echo -n "hedge     "; bucket "$HEDGE_JSONL"
} | tee "$REPORT"

echo
echo "wrote $REPORT"
