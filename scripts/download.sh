#!/usr/bin/env bash
set -euo pipefail

JUMP="PCPGnshm@blp01.ccni.rpi.edu"
REMOTE="PCPGnshm@dcsfen01"
REMOTE_DIR="scratch/final_project"

mkdir -p final_project

scp -O -J "$JUMP" -r "$REMOTE:$REMOTE_DIR/results" final_project/

echo "Downloaded results into final_project/results"
