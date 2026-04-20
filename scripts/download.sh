#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PROJECT_NAME="final_project"
JUMP="PCPGlkht@blp01.ccni.rpi.edu"
REMOTE="PCPGlkht@dcsfen01"
REMOTE_DIR="scratch/$PROJECT_NAME"

scp -O -J "$JUMP" -r "$REMOTE:$REMOTE_DIR/results" "$ROOT_DIR/"

echo "Downloaded results into $ROOT_DIR/results"
