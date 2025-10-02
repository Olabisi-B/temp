#!/bin/bash
set -euo pipefail

usage() {
  echo "Usage: $0 -g genomic_input -r transcriptomic_input -d kraken2_db_path [-o output_dir] [-b bracken_threshold] [-t top_species]"
  echo "  -g  Genomic input (FASTQ file, accession, or accession list)"
  echo "  -r  Transcriptomic input (FASTQ file, accession, or accession list)"
  echo "  -d  Path to Kraken2 database"
  echo "  -o  Output directory (default: output)"
  echo "  -b  Bracken threshold (default: 10)"
  echo "  -t  Top N species to keep (default: 500)"
  exit 1
}

# ---- Defaults ----
OUTDIR="output"
TOP_N=500
BRACKEN_THRESHOLD=10
DB=""

# ---- Parse options ----
while getopts "g:r:o:d:t:b:" opt; do
  case "$opt" in
    g) GENOMIC_INPUT="$OPTARG" ;;
    r) TRANSCRIPT_INPUT="$OPTARG" ;;
    o) OUTDIR="$OPTARG" ;;
    d) DB="$OPTARG" ;;
    t) TOP_N="$OPTARG" ;;
    b) BRACKEN_THRESHOLD="$OPTARG" ;;
    *) usage ;;
  esac
done

# ---- Validate inputs ----
if [[ -z "${GENOMIC_INPUT:-}" || -z "${TRANSCRIPT_INPUT:-}" ]]; then
  echo "Error: Both genomic (-g) and transcriptomic (-r) inputs are required."
  usage
fi

if [[ -z "$DB" ]]; then
  echo "Error: Kraken2 database path required (-d)"
  usage
fi

# ---- Setup paths ----
GENOMIC_DIR="$OUTDIR/genomic"
TRANSCRIPT_DIR="$OUTDIR/transcriptomic"
mkdir -p "$GENOMIC_DIR" "$TRANSCRIPT_DIR"

# ---- Helper: run pipeline ----
run_pipeline() {
  local INPUT=$1
  local LABEL=$2
  local WORKDIR=$3

  local FASTQ="$WORKDIR/fastq"
  local KRAKEN="$WORKDIR/kraken"
  local BRACKEN="$WORKDIR/bracken"
  local TMP="$WORKDIR/tmp"
  local CSV="$WORKDIR/${LABEL}_abundance.csv"

  mkdir -p "$FASTQ" "$KRAKEN" "$BRACKEN" "$TMP"
  > "$TMP/all_species.tsv"

  echo "=== Running pipeline for $LABEL ($INPUT) ==="

  # --- Detect FASTQ or accession ---
  if [[ "$INPUT" == *.fastq || "$INPUT" == *.fastq.gz ]]; then
    echo "Detected FASTQ file input: $INPUT"
    ACCESSION="$LABEL"
    FQ="$INPUT"
    READ_LEN=$(gunzip -c "$FQ" | sed -n 2p | wc -c)
    READ_LEN=$((READ_LEN - 1))

    kraken2 --db "$DB" "$FQ" \
      --threads 4 --use-names \
      --report "$KRAKEN/${ACCESSION}_report.txt" \
      --output "$KRAKEN/${ACCESSION}_output.txt"

  else
    echo "Detected accession input: $INPUT"
    ACCESSION="$INPUT"
    ACC_DIR="$FASTQ/$ACCESSION"
    mkdir -p "$ACC_DIR"

    prefetch "$ACCESSION" --progress --output-directory "$ACC_DIR"
    SRA_FILE=$(find "$ACC_DIR" -name "*.sra" | head -n 1)
    fasterq-dump "$SRA_FILE" -O "$ACC_DIR" --temp "$ACC_DIR/tmp"

    FQ1="$ACC_DIR/${ACCESSION}_1.fastq"
    FQ2="$ACC_DIR/${ACCESSION}_2.fastq"
    FQ="$ACC_DIR/${ACCESSION}.fastq"

    if [[ -f "$FQ1" && -f "$FQ2" ]]; then
      READ_LEN=$(awk 'NR==2 {print length($0)}' "$FQ1")
      kraken2 --db "$DB" --paired "$FQ1" "$FQ2" \
        --threads 4 --use-names \
        --report "$KRAKEN/${ACCESSION}_report.txt" \
        --output "$KRAKEN/${ACCESSION}_output.txt"
    else
      READ_LEN=$(awk 'NR==2 {print length($0)}' "$FQ")
      kraken2 --db "$DB" "$FQ" \
        --threads 4 --use-names \
        --report "$KRAKEN/${ACCESSION}_report.txt" \
        --output "$KRAKEN/${ACCESSION}_output.txt"
    fi
  fi

  # --- Bracken ---
  bracken -d "$DB" \
    -i "$KRAKEN/${ACCESSION}_report.txt" \
    -o "$BRACKEN/${ACCESSION}_bracken.txt" \
    -l S -r "$READ_LEN" -t "$BRACKEN_THRESHOLD"

  awk 'NR>1 {gsub(/ /, "_", $1); printf "%s_%s\t%s\n", $1, $2, $NF}' "$BRACKEN/${ACCESSION}_bracken.txt" \
    | sort -k2,2nr | head -n "$TOP_N" > "$TMP/${ACCESSION}_species.tsv"

  cut -f1 "$TMP/${ACCESSION}_species.tsv" >> "$TMP/all_species.tsv"

  # --- Build CSV ---
  sort "$TMP/all_species.tsv" | uniq > "$TMP/species_headers.txt"

  echo -n "sample" > "$CSV"
  while read -r species; do
    echo -n ",$species" >> "$CSV"
  done < "$TMP/species_headers.txt"
  echo >> "$CSV"

  ROW="$ACCESSION"
  while read -r species; do
    value=$(awk -v sp="$species" -F"\t" '$1 == sp {print $2}' "$TMP/${ACCESSION}_species.tsv")
    ROW="${ROW},${value:-0}"
  done < "$TMP/species_headers.txt"
  echo "$ROW" >> "$CSV"

  echo "Finished $LABEL. Output CSV: $CSV"
}

# ---- Run both pipelines ----
run_pipeline "$GENOMIC_INPUT" "genomic" "$GENOMIC_DIR"
run_pipeline "$TRANSCRIPT_INPUT" "transcriptomic" "$TRANSCRIPT_DIR"

# ---- Compare results ----
GENOMIC_CSV="$GENOMIC_DIR/genomic_abundance.csv"
TRANSCRIPT_CSV="$TRANSCRIPT_DIR/transcriptomic_abundance.csv"

echo "=== Similarity Review ==="
GEN_SPECIES=$(cut -d',' -f2- "$GENOMIC_CSV" | head -n1 | tr ',' '\n' | sort)
TRN_SPECIES=$(cut -d',' -f2- "$TRANSCRIPT_CSV" | head -n1 | tr ',' '\n' | sort)

COMMON=$(comm -12 <(echo "$GEN_SPECIES") <(echo "$TRN_SPECIES") | wc -l)
TOTAL=$(comm -3 <(echo "$GEN_SPECIES") <(echo "$TRN_SPECIES") | wc -l)

echo "Shared species: $COMMON"
echo "Unique species: $TOTAL"
