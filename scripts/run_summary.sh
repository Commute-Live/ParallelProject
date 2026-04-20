#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 4 ]]; then
  echo "Usage: $0 <input_bin> <city_report_csv> <bucket_report_csv> <stdout_log>" >&2
  exit 1
fi

INPUT_BIN="$1"
CITY_REPORT="$2"
BUCKET_REPORT="$3"
STDOUT_LOG="$4"

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
EXE="${EXE:-$ROOT_DIR/transit-analysis}"
SUMMARY_RANKS="${SUMMARY_RANKS:-4}"
SUMMARY_BACKEND="${SUMMARY_BACKEND:-cuda}"
SUMMARY_RECORDS="${SUMMARY_RECORDS:-0}"

if [[ ! -x "$EXE" ]]; then
  echo "ERROR: executable $EXE not found" >&2
  exit 1
fi

mkdir -p "$(dirname "$CITY_REPORT")" "$(dirname "$BUCKET_REPORT")" "$(dirname "$STDOUT_LOG")"

cmd=(
  mpirun --bind-to core --report-bindings -np "$SUMMARY_RANKS"
  "$EXE"
  --input "$INPUT_BIN"
  --backend "$SUMMARY_BACKEND"
  --city-report "$CITY_REPORT"
  --bucket-report "$BUCKET_REPORT"
)

if [[ "$SUMMARY_RECORDS" -gt 0 ]]; then
  cmd+=(--records "$SUMMARY_RECORDS")
fi

"${cmd[@]}" | tee "$STDOUT_LOG"
