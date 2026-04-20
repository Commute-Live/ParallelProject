#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PROJECT_NAME="final_project"
JUMP="PCPGNAMEHEREt@blp01.ccni.rpi.edu"
REMOTE="PCPGNAMEHERE@dcsfen01"
REMOTE_PARENT="~/scratch"
DATASET_BIN="$ROOT_DIR/data/processed/transit_trip_records.bin"
DATASET_MANIFEST="$ROOT_DIR/data/processed/transit_trip_records_manifest.json"
STAGE_ROOT="$(mktemp -d)"
STAGE_DIR="$STAGE_ROOT/$PROJECT_NAME"

cleanup() {
  rm -rf "$STAGE_ROOT"
}

trap cleanup EXIT

if [[ "${SKIP_DATASET_UPLOAD:-0}" != "1" ]]; then
  if [[ ! -f "$DATASET_BIN" || ! -f "$DATASET_MANIFEST" ]]; then
    echo "ERROR: processed dataset not found." >&2
    echo "Run 'bash final_project/scripts/prepare_dataset.sh' first." >&2
    echo "If you intentionally want a code-only upload, rerun with SKIP_DATASET_UPLOAD=1." >&2
    exit 1
  fi
fi

mkdir -p "$STAGE_DIR/data/processed" "$STAGE_DIR/results"

cp "$ROOT_DIR/Makefile" "$ROOT_DIR/requirements.txt" "$STAGE_DIR/"

if [[ -f "$ROOT_DIR/README.md" ]]; then
  cp "$ROOT_DIR/README.md" "$STAGE_DIR/"
fi

cp -R "$ROOT_DIR/config" "$STAGE_DIR/"
cp -R "$ROOT_DIR/include" "$STAGE_DIR/"
cp -R "$ROOT_DIR/src" "$STAGE_DIR/"
cp -R "$ROOT_DIR/scripts" "$STAGE_DIR/"

if [[ -d "$ROOT_DIR/report" ]]; then
  cp -R "$ROOT_DIR/report" "$STAGE_DIR/"
fi

if [[ "${SKIP_DATASET_UPLOAD:-0}" != "1" && -f "$DATASET_BIN" ]]; then
  cp "$DATASET_BIN" "$DATASET_MANIFEST" "$STAGE_DIR/data/processed/"
fi

scp -O -J "$JUMP" -r "$STAGE_DIR" "$REMOTE:$REMOTE_PARENT/"

echo "Upload complete."
echo ""
echo "Next steps on AiMOS:"
echo "  ssh -J $JUMP $REMOTE"
echo "  cd ~/scratch/$PROJECT_NAME"
echo "  ls -lh data/processed/"
echo "  cd scripts"
echo "  sbatch sbatch_summary.sh"
echo "  sbatch sbatch_strong.sh"
echo "  sbatch sbatch_weak.sh"
