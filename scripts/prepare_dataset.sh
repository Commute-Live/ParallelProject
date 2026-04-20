#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

python3 "$ROOT_DIR/scripts/download_gtfs.py" \
  --config "$ROOT_DIR/config/feeds.json" \
  --raw-dir "$ROOT_DIR/data/raw"

python3 "$ROOT_DIR/scripts/build_dataset.py" \
  --config "$ROOT_DIR/config/feeds.json" \
  --raw-dir "$ROOT_DIR/data/raw" \
  --out-dir "$ROOT_DIR/data/processed"
