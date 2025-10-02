#!/bin/bash
ACCESSION="$1"
SCRATCH_DIR="$2"

echo "[INFO] Starting pipeline for $ACCESSION in $SCRATCH_DIR"
mkdir -p "$SCRATCH_DIR"
cd "$SCRATCH_DIR" || { echo "[ERROR] Cannot cd to $SCRATCH_DIR"; exit 1; }

download_fastq() {
  local run_acc=$1
  echo "[INFO] Downloading FASTQ for run accession $run_acc"
  fasterq-dump "$run_acc" --threads 16 -O "$SCRATCH_DIR" || {
    echo "[ERROR] fasterq-dump failed for $run_acc"
    return 1
  }
  return 0
}

# If accession is already a run accession
if [[ "$ACCESSION" =~ ^SRR|^ERR|^DRR ]]; then
  if ! download_fastq "$ACCESSION"; then
    echo "[ERROR] Failed to download FASTQ for $ACCESSION"
    exit 1
  fi
else
  # Otherwise, try to resolve run accessions for this sample accession from NCBI datasets API
  RUNS=$(curl -s "https://api.ncbi.nlm.nih.gov/datasets/v1alpha/sequence/run/accessions/accession/${ACCESSION}" | jq -r '.runs[].accession' 2>/dev/null)

  if [ -z "$RUNS" ]; then
    echo "[WARN] No run accessions found from datasets API for $ACCESSION, trying Entrez Direct esearch fallback..."

    # Use esearch + elink from biosample DB as fallback
    RUNS=$(esearch -db biosample -query "$ACCESSION" 2>/dev/null | elink -target sra 2>/dev/null | efetch -format runinfo 2>/dev/null | cut -d',' -f1 | tail -n +2 || true)

    if [ -z "$RUNS" ]; then
      echo "[WARN] No run accessions found for $ACCESSION with esearch fallback either."
      touch no_runs_found.txt
      exit 0
    fi
  fi

  FIRST_RUN=$(echo "$RUNS" | head -n1)
  if ! download_fastq "$FIRST_RUN"; then
    echo "[ERROR] Failed to download FASTQ for run accession $FIRST_RUN resolved from $ACCESSION"
    exit 1
  fi
fi

# Check if any FASTQ files exist after download
if ! ls *.fastq &>/dev/null; then
  echo "[ERROR] No FASTQ files found after download"
  exit 1
fi

# ---- Run Kraken2 ----
KRAKEN_DB=""
KRAKEN_OUT="${ACCESSION}_kraken.tsv"
KRAKEN_REPORT="${ACCESSION}_report.txt"

echo "[INFO] Running Kraken2"
kraken2 --db "$KRAKEN_DB" --threads 16 --use-names \
  --report "$KRAKEN_REPORT" --output "$KRAKEN_OUT" *.fastq

# ---- Run Bracken: Species ----
BRACKEN_SPECIES_OUT="${ACCESSION}_bracken_species.txt"
echo "[INFO] Running Bracken (Species)"
bracken -d "$KRAKEN_DB" -i "$KRAKEN_REPORT" -o "$BRACKEN_SPECIES_OUT" -r 150 -l S -t 0 || {
  echo "[ERROR] Bracken failed at species level"
  exit 1
}

# ---- Run Bracken: Genus ----
BRACKEN_GENUS_OUT="${ACCESSION}_bracken_genus.txt"
echo "[INFO] Running Bracken (Genus)"
bracken -d "$KRAKEN_DB" -i "$KRAKEN_REPORT" -o "$BRACKEN_GENUS_OUT" -r 150 -l G -t 0 || {
  echo "[ERROR] Bracken failed at genus level"
  exit 1
}

# ---- Merge outputs into final CSV ----
BRACKEN_COMBINED_OUT="${ACCESSION}_bracken_combined.csv"
echo "[INFO] Merging genus + species output to $BRACKEN_COMBINED_OUT (sorted by abundance)"

if [[ -s "$BRACKEN_GENUS_OUT" && -s "$BRACKEN_SPECIES_OUT" ]]; then
  {
    echo "Genus,Species,SpeciesTaxID,FractionalAbundance"
    paste \
      <(cut -f1 "$BRACKEN_GENUS_OUT") \
      <(cut -f1 "$BRACKEN_SPECIES_OUT") \
      <(cut -f2 "$BRACKEN_SPECIES_OUT") \
      <(awk -F'\t' '{print $NF}' "$BRACKEN_SPECIES_OUT") \
      | sort -t$'\t' -k4,4gr
  } > "$BRACKEN_COMBINED_OUT"
else
  echo "[WARNING] Missing or empty Bracken output. Combined CSV not created for $ACCESSION"
fi

# ---- Clean up FASTQ ----
echo "[INFO] Deleting FASTQ to save space"
rm -v *.fastq || echo "[WARN] No FASTQ files found to delete."

# ---- Save results ----
RESULTS_DIR=""
mkdir -p "$RESULTS_DIR"

cp -v "$KRAKEN_REPORT" "$BRACKEN_SPECIES_OUT" "$BRACKEN_GENUS_OUT" "$BRACKEN_COMBINED_OUT" "$RESULTS_DIR/" || {
  echo "[WARNING] Copy failed. Some files may be missing."
}

echo "[INFO] Pipeline complete. Results saved to $RESULTS_DIR"

