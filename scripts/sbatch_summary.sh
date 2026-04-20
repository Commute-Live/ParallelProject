#!/bin/bash
#SBATCH --job-name=transit_summary
#SBATCH --output=../results/transit_summary_%j.out
#SBATCH --nodes=1
#SBATCH --ntasks=4
#SBATCH --ntasks-per-node=4
#SBATCH --gres=gpu:4
#SBATCH --partition=el8-rpi
#SBATCH --time=00:20:00

set -euo pipefail

module purge
module load xl_r spectrum-mpi cuda

PROJECT_DIR="${HOME}/scratch/final_project"
DATASET="${PROJECT_DIR}/data/processed/transit_trip_records.bin"

cd "$PROJECT_DIR"

make clean
make

mkdir -p results

if [[ ! -f "$DATASET" ]]; then
  echo "Dataset not found: $DATASET"
  echo "Prepare the dataset locally and rerun scripts/upload.sh before submitting this job."
  exit 1
fi

bash scripts/run_summary.sh \
  "$DATASET" \
  "$PROJECT_DIR/results/city_summary.csv" \
  "$PROJECT_DIR/results/bucket_summary.csv" \
  "$PROJECT_DIR/results/summary_stdout.txt"
