#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 3 ]]; then
  echo "Usage: $0 <strong|weak> <input_bin> <results_csv> [extra analyzer args...]" >&2
  exit 1
fi

STUDY="$1"
INPUT_BIN="$2"
RESULTS_CSV="$3"
shift 3

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
EXE="${EXE:-$ROOT_DIR/transit-analysis}"
RANKS=(2 4 8 16)
BACKENDS=(cpu cuda)
STRONG_RECORDS="${STRONG_RECORDS:-400000}"
WEAK_RECORDS_PER_RANK="${WEAK_RECORDS_PER_RANK:-150000}"

if [[ ! -x "$EXE" ]]; then
  echo "ERROR: executable $EXE not found" >&2
  exit 1
fi

if [[ ! -f "$INPUT_BIN" ]]; then
  echo "ERROR: input dataset $INPUT_BIN not found" >&2
  exit 1
fi

mkdir -p "$(dirname "$RESULTS_CSV")"

if [[ ! -s "$RESULTS_CSV" ]]; then
  echo "study,backend,ranks,records,cities,io_seconds,boundary_seconds,compute_seconds,reduce_seconds,total_seconds,mean_peak_share,mean_headway_seconds,mean_headway_cv,status" > "$RESULTS_CSV"
fi

TMPFILE="$(mktemp)"
trap 'rm -f "$TMPFILE"' EXIT

for backend in "${BACKENDS[@]}"; do
  for ranks in "${RANKS[@]}"; do
    if [[ "$STUDY" == "strong" ]]; then
      records="$STRONG_RECORDS"
    elif [[ "$STUDY" == "weak" ]]; then
      records=$((WEAK_RECORDS_PER_RANK * ranks))
    else
      echo "ERROR: study must be strong or weak" >&2
      exit 1
    fi

    echo "Running study=$STUDY backend=$backend ranks=$ranks records=$records"
    mpirun --bind-to core --report-bindings -np "$ranks" \
      "$EXE" \
      --input "$INPUT_BIN" \
      --backend "$backend" \
      --records "$records" \
      --csv \
      "$@" \
      > "$TMPFILE" 2>&1

    row="$(grep '^CSVROW,' "$TMPFILE" | tail -n 1 | cut -d',' -f2- || true)"
    if [[ -z "$row" ]]; then
      cat "$TMPFILE" >&2
      echo "ERROR: analyzer did not emit a CSV row" >&2
      exit 1
    fi

    echo "$STUDY,$row" >> "$RESULTS_CSV"
  done
done

echo "Experiment sweep complete: $RESULTS_CSV"
