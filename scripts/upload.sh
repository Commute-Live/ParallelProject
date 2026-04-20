#!/usr/bin/env bash
set -euo pipefail

JUMP="PCPGnshm@blp01.ccni.rpi.edu"
REMOTE="PCPGnshm@dcsfen01"
REMOTE_PARENT="~/scratch"
DATASET_BIN="final_project/data/processed/transit_trip_records.bin"
DATASET_MANIFEST="final_project/data/processed/transit_trip_records_manifest.json"
STAGE_ROOT="$(mktemp -d)"
STAGE_DIR="$STAGE_ROOT/final_project"

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

cp final_project/Makefile final_project/requirements.txt "$STAGE_DIR/"

if [[ -f final_project/README.md ]]; then
  cp final_project/README.md "$STAGE_DIR/"
fi

cp -R final_project/config "$STAGE_DIR/"
cp -R final_project/include "$STAGE_DIR/"
cp -R final_project/src "$STAGE_DIR/"
cp -R final_project/scripts "$STAGE_DIR/"

if [[ -d final_project/report ]]; then
  cp -R final_project/report "$STAGE_DIR/"
fi

if [[ "${SKIP_DATASET_UPLOAD:-0}" != "1" && -f "$DATASET_BIN" ]]; then
  cp "$DATASET_BIN" "$DATASET_MANIFEST" "$STAGE_DIR/data/processed/"
fi

scp -O -J "$JUMP" -r "$STAGE_DIR" "$REMOTE:$REMOTE_PARENT/"

echo "Upload complete."
echo ""
echo "Next steps on AiMOS:"
echo "  ssh -J $JUMP $REMOTE"
echo "  cd ~/scratch/final_project"
echo "  ls -lh data/processed/"
echo "  cd scripts"
echo "  sbatch sbatch_summary.sh"
echo "  sbatch sbatch_strong.sh"
echo "  sbatch sbatch_weak.sh"
