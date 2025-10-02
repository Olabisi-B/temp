#!/bin/bash -l
#SBATCH --job-name=
#SBATCH --output=
#SBATCH --error=
#SBATCH --ntasks=
#SBATCH --cpus-per-task=
#SBATCH --mem=
#SBATCH --time=
set -euo pipefail

# ---- Load modules ----
module purge
module load sratools
module load kraken
module load bracken

# ---- Setup paths ----
cd /home/ || exit 1
mkdir -p logs results

SCRATCH_BASE=""
ACCESSIONS="${ACCESSIONS:-250707.txt}"
KRAKEN2_DB=""

# ---- Get the SRR accession from array task ----
INDEX=$(( OFFSET + SLURM_ARRAY_TASK_ID ))
ACCESSION=$(sed -n "$((INDEX + 1))p" "$ACCESSIONS")

if [ -z "$ACCESSION" ]; then
  echo "[INFO] No accession at index $INDEX; exiting."
  exit 0
fi

# ---- Create scratch dir for this task ----
SCRATCH_DIR="$SCRATCH_BASE/${SLURM_JOB_ID}_${SLURM_ARRAY_TASK_ID}"
mkdir -p "$SCRATCH_DIR"

# ---- Run pipeline script ----
echo "[INFO] Running pipeline for $ACCESSION in $SCRATCH_DIR"
bash ./pipeline.sh "$ACCESSION" "$SCRATCH_DIR"
echo "[INFO] Pipeline completed for $ACCESSION"


