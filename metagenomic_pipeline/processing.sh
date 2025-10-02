#!/bin/bash
set -euo pipefail

usage() {
  echo "Usage: $0 -i <input (fastq|accession|file)> -o <output_dir> -d <kraken2_db_path> [-b bracken_threshold] [--keep-fastq]"
  exit 1
}

# ---- Defaults ----
OUTDIR="output"
DB=""
BRACKEN_THRESHOLD=10
KEEP_FASTQ=false

# ---- Parse options ----
while [[ $# -gt 0 ]]; do
  case "$1" in
    -i) INPUT="$2"; shift 2 ;;
    -o) OUTDIR="$2"; shift 2 ;;
    -d) DB="$2"; shift 2 ;;
    -b) BRACKEN_THRESHOLD="$2"; shift 2 ;;
    --keep-fastq) KEEP_FASTQ=true; shift ;;
    *) usage ;;
  esac
done

# ---- Validate inputs ----
if [[ -z "${INPUT:-}" ]]; then
  echo "Error: Input required (-i)"
  usage
fi
if [[ -z "$DB" ]]; then
  echo "Error: Kraken2 database path required (-d)"
  usage
fi

# ---- Detect FASTQ ----
is_fastq() {
  [[ "$1" == *.fastq || "$1" == *.fastq.gz ]]
}

# ---- FASTQ input case ----
if is_fastq "$INPUT"; then
  SAMPLE="sample"
  SAMPLE_DIR="$OUTDIR/$SAMPLE"
  mkdir -p "$SAMPLE_DIR"

  echo "Processing FASTQ: $INPUT"
  if [[ "$INPUT" == *.gz ]]; then
    READ_LEN=$(gunzip -c "$INPUT" | sed -n 2p | wc -c)
  else
    READ_LEN=$(sed -n 2p "$INPUT" | wc -c)
  fi
  READ_LEN=$((READ_LEN - 1))

  kraken2 --db "$DB" "$INPUT" \
    --threads 4 --use-names \
    --report "$SAMPLE_DIR/kraken_report.txt" \
    --output "$SAMPLE_DIR/kraken_output.txt"

  bracken -d "$DB" \
    -i "$SAMPLE_DIR/kraken_report.txt" \
    -o "$SAMPLE_DIR/bracken.txt" \
    -l S -r "$READ_LEN" -t "$BRACKEN_THRESHOLD"

  echo "Finished FASTQ → $SAMPLE_DIR"
  exit 0
fi

# ---- SRA accession list case ----
ACCESSION_FILE="$INPUT"
while read -r ACC; do
  echo "Processing accession: $ACC"
  SAMPLE_DIR="$OUTDIR/$ACC"
  mkdir -p "$SAMPLE_DIR"

  prefetch "$ACC" --progress --output-directory "$SAMPLE_DIR"

  SRA_FILE=$(find "$SAMPLE_DIR" -name "*.sra" | head -n 1)
  if [[ -z "$SRA_FILE" || ! -f "$SRA_FILE" ]]; then
    echo "SRA file not found for $ACC"
    continue
  fi

  fasterq-dump "$SRA_FILE" -O "$SAMPLE_DIR" --temp "$SAMPLE_DIR/tmp"

  FQ1="$SAMPLE_DIR/${ACC}_1.fastq"
  FQ2="$SAMPLE_DIR/${ACC}_2.fastq"
  FQ="$SAMPLE_DIR/${ACC}.fastq"

  if [[ -f "$FQ1" && -f "$FQ2" ]]; then
    READ_LEN=$(awk 'NR==2 {print length($0)}' "$FQ1")
    kraken2 --db "$DB" --paired "$FQ1" "$FQ2" \
      --threads 4 --use-names \
      --report "$SAMPLE_DIR/kraken_report.txt" \
      --output "$SAMPLE_DIR/kraken_output.txt"
  elif [[ -f "$FQ" ]]; then
    READ_LEN=$(awk 'NR==2 {print length($0)}' "$FQ")
    kraken2 --db "$DB" "$FQ" \
      --threads 4 --use-names \
      --report "$SAMPLE_DIR/kraken_report.txt" \
      --output "$SAMPLE_DIR/kraken_output.txt"
  else
    echo "No FASTQ found for $ACC, skipping."
    continue
  fi

  bracken -d "$DB" \
    -i "$SAMPLE_DIR/kraken_report.txt" \
    -o "$SAMPLE_DIR/bracken.txt" \
    -l S -r "$READ_LEN" -t "$BRACKEN_THRESHOLD"

  # Remove FASTQs unless user requested to keep them
  if [[ "$KEEP_FASTQ" == false ]]; then
    echo "Deleting FASTQ files for $ACC (use --keep-fastq to keep)"
    rm -f "$SAMPLE_DIR"/*.fastq
  fi

  echo "Finished accession → $SAMPLE_DIR"
done < "$ACCESSION_FILE"
