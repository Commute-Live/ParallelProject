#!/bin/bash
#SBATCH --job-name=transit_weak
#SBATCH --output=../results/transit_weak_%j.out
#SBATCH --nodes=4
#SBATCH --ntasks=16
#SBATCH --ntasks-per-node=4
#SBATCH --gres=gpu:4
#SBATCH --partition=el8-rpi
#SBATCH --time=00:30:00

set -euo pipefail

module purge
module load xl_r spectrum-mpi cuda

PROJECT_DIR="${HOME}/scratch/final_project"
DATASET="${PROJECT_DIR}/data/processed/transit_trip_records.bin"
RESULTS_CSV="${PROJECT_DIR}/results/experiment_results.csv"

cd "$PROJECT_DIR"

make clean
make

mkdir -p results

if [[ ! -f "$DATASET" ]]; then
  echo "Dataset not found: $DATASET"
  echo "Prepare the dataset locally and rerun scripts/upload.sh before submitting this job."
  exit 1
fi

bash scripts/run_experiments.sh weak "$DATASET" "$RESULTS_CSV"
