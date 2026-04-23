# Parallel Transit Analysis

This project analyzes large transit schedule data in parallel on AiMOS using `MPI`, `MPI-I/O`, and `CUDA`. It runs summary and scaling jobs and writes the output files into the `results/` folder.

## How to Run

1. Unzip the project on AiMOS and go into the project folder.

```bash
unzip final_project.zip
cd final_project
```

2. Submit the jobs. You can either use the `Makefile` shortcuts or run the `sbatch` scripts directly.

Using `sbatch` directly:

```bash
cd scripts
sbatch sbatch_summary.sh
sbatch sbatch_strong.sh
sbatch sbatch_weak.sh
```

Using `make` from the project root:

```bash
make summary
make strong
make weak
```

Or submit all three at once:

```bash
make submit
```


3. Go back to the project folder and check the `results/` folder for outputs.

```bash
ls results
```

The main outputs are job logs, summary CSV files, and `results/experiment_results.csv`.
