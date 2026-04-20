# Final Project: Massively Parallel Transit Schedule Analytics

This project builds a hybrid `MPI` + `MPI-I/O` + `CUDA` pipeline for analyzing large GTFS transit feeds across multiple cities on AiMOS. It is designed to satisfy the course requirement that the system be genuinely interactive and not embarrassingly parallel:

- `MPI-I/O` performs collective reads of one shared binary dataset.
- `MPI` partitions the work, exchanges boundary state for headway calculations, and reduces global aggregates.
- `CUDA` accelerates per-trip analytics on each rank's GPU.

The implementation currently focuses on **schedule-based service analysis** from GTFS static feeds. That means the project measures scheduled service intensity and scheduling regularity, not passenger ridership. In the paper, describe peak/non-peak behavior as a **service-volume proxy** unless you later add GTFS-Realtime or APC/AFC data.

## What the code does

1. Downloads GTFS feeds for New York, Chicago, Boston, New Jersey, and Philadelphia.
2. Converts raw GTFS data into a fixed-width binary `trip_record` dataset sorted by:
   `city_id, route_hash, direction_id, start_secs, trip_hash`
3. Uses `MPI_File_read_at_all` to collectively load slices of that dataset.
4. Uses `MPI_Sendrecv` to exchange one boundary trip record between neighboring ranks so headway calculations are correct across partition boundaries.
5. Runs either:
   - `--backend cpu`: distributed MPI analysis on CPUs only
   - `--backend cuda`: distributed MPI analysis with GPU local compute
6. Aggregates global statistics with `MPI_Reduce`.
7. Writes optional city-level and bucket-level summaries for plots and report tables.

## Metrics produced

- departures per time bucket by city
- peak-share by city
- route-type counts by city
- trip duration statistics by city
- headway mean and coefficient of variation by city
- bunching rate (`< 5 min`)
- wide-gap rate (`> 20 min`)
- MPI-I/O, boundary exchange, compute, reduction, and total runtime

## Project layout

```text
final_project/
├── Makefile
├── README.md
├── config/feeds.json
├── include/
├── src/
├── scripts/
├── data/
│   ├── raw/
│   └── processed/
├── results/
└── report/
```

## Core files

- `src/transit_analysis_mpi.c`: MPI orchestration, collective I/O, reductions, reporting
- `src/transit_analysis_cuda.cu`: CUDA kernel and device-side aggregation
- `scripts/download_gtfs.py`: download GTFS zip files
- `scripts/build_dataset.py`: convert GTFS to fixed-width binary trip records
- `scripts/run_experiments.sh`: strong/weak scaling sweeps for `cpu` and `cuda`
- `scripts/run_summary.sh`: one representative run that emits city/bucket summaries
- `scripts/analyze_results.py`: generate figures from experiment CSVs
- `report/report.tex`: ACM-format paper draft

## Local workflow

If you only want to inspect or edit the project locally, no special setup is needed. Full compilation is AiMOS-specific because the executable depends on Spectrum MPI and CUDA.

Python syntax for the helper scripts was validated locally. Full `mpicc`/`nvcc` compilation was not possible in this environment.

The plotting helper requires:

```bash
python3 -m pip install -r final_project/requirements.txt
```

## Dataset preparation

Prepare the dataset on your own machine before uploading anything to AiMOS. The cluster-side front end is behind a firewall, so the download step should not be part of the remote workflow.

From the repo root:

```bash
bash final_project/scripts/prepare_dataset.sh
```

This writes:

- `final_project/data/processed/transit_trip_records.bin`
- `final_project/data/processed/transit_trip_records_manifest.json`

You only need to rebuild the dataset when you change the feed list or preprocessing logic.

## AiMOS workflow

### 1. Upload the project

From the repo root:

```bash
bash final_project/scripts/upload.sh
```

### 2. Log into AiMOS

```bash
ssh -J PCPGnshm@blp01.ccni.rpi.edu PCPGnshm@dcsfen01
cd scratch/final_project
```

### 3. Verify the uploaded dataset

Do not prepare GTFS data on AiMOS. Just confirm that the processed files are already present:

```bash
ls -lh data/processed/
```

### 4. Run the summary job

This gives you one city-level report and one bucket-level report for the paper.

```bash
cd scripts
sbatch sbatch_summary.sh
```

### 5. Run scaling jobs

```bash
sbatch sbatch_strong.sh
sbatch sbatch_weak.sh
```

Both scripts append to:

`results/experiment_results.csv`

### 6. Download results

Back on your local machine:

```bash
bash final_project/scripts/download.sh
python3 final_project/scripts/analyze_results.py \
  --results final_project/results/experiment_results.csv \
  --manifest final_project/data/processed/transit_trip_records_manifest.json \
  --city-report final_project/results/city_summary.csv
```

Plots are written into:

`final_project/results/plots/`

## Strong and weak scaling definitions

- Strong scaling: fixed total trip-record count, increasing MPI ranks
- Weak scaling: fixed trip-records per rank, increasing MPI ranks and total trip-record count

Defaults in `scripts/run_experiments.sh`:

- `STRONG_RECORDS=400000`
- `WEAK_RECORDS_PER_RANK=150000`

You can override them when launching the job:

```bash
STRONG_RECORDS=800000 sbatch sbatch_strong.sh
WEAK_RECORDS_PER_RANK=250000 sbatch sbatch_weak.sh
```

## Suggested report framing

Be precise about the claims:

- Strong claim you can support now:
  This project analyzes large, multi-city transit schedules in parallel and compares CPU-only MPI against hybrid MPI+CUDA while measuring collective I/O and communication overhead.

- Claim you should avoid unless you add more data:
  Direct passenger usage inference.

Safer language:

- "scheduled service intensity"
- "peak-period service concentration"
- "headway regularity"
- "service reliability proxy from schedule structure"

## Recommended paper narrative

1. GTFS feeds are large, multi-file, and expensive to normalize serially.
2. A fixed-width binary representation makes collective MPI-I/O practical.
3. Sorting by route and direction creates a distributed adjacency problem for headways, so boundary exchange is required.
4. City-level statistics require MPI reductions and are therefore not embarrassingly parallel.
5. The CUDA backend accelerates bucketization and headway-stat accumulation relative to the CPU-only MPI baseline.

## Caveats

- The current preprocessing stage uses GTFS static feeds only.
- Because AiMOS is behind a firewall, GTFS acquisition must happen locally before upload.
- `calendar_dates.txt` is not yet merged into service masks; if you want holiday-specific analysis, extend `build_dataset.py`.
- `frequencies.txt` is not expanded yet; some frequency-based feeds may therefore undercount trips.
- GTFS-Realtime support is not yet integrated into the binary dataset. That is a strong future-work section for the paper.

## Next high-value extensions

- incorporate `frequencies.txt`
- merge `calendar_dates.txt` exceptions
- add GTFS-Realtime trip delay ingestion
- compare `MPI_File_read_at_all` against serial or rank-0 read/broadcast loading
- experiment with node-local NVMe staging for the binary dataset
