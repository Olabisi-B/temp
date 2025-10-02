#!/bin/bash
set -euo pipefail

usage() {
  echo "Usage: $0 -i <accession_or_accession_file> [-o output_dir] [-d kraken2_db_path] [-t top_species]"
  exit 1
}

# ---- Defaults ----
OUTDIR="output"
DB=""

# ---- Parse options ----
while getopts "i:o:d:t:" opt; do
  case "$opt" in
    i) INPUT="$OPTARG" ;;
    o) OUTDIR="$OPTARG" ;;
    d) DB="$OPTARG" ;;
    t) TOP_N="$OPTARG" ;;
    *) usage ;;
  esac
done

# ---- Validate inputs ----
if [[ -z "${INPUT:-}" ]]; then
  echo "Error: Input accession or file required (-i)"
  usage
fi

if [[ -z "$DB" ]]; then
  echo "Error: Kraken2 database path required (-d)"
  usage
fi

# ---- Setup paths ----
FASTQ="$OUTDIR/fastq"
KRAKEN="$OUTDIR/kraken"
BRACKEN="$OUTDIR/bracken"
TMP="$OUTDIR/tmp"
CSV="$OUTDIR/abundance_matrix.csv"

mkdir -p "$FASTQ" "$KRAKEN" "$BRACKEN" "$TMP"
> "$TMP/all_species.tsv"

# ---- Handle input: single accession vs file ----
if [[ -f "$INPUT" ]]; then
  ACCESSION_FILE="$INPUT"
else
  ACCESSION_FILE=$(mktemp)
  trap 'rm -f "$ACCESSION_FILE"' EXIT

  # If SRX/SRP/SRS, resolve to SRR
  if [[ "$INPUT" =~ ^SR[XP] || "$INPUT" =~ ^SRS ]]; then
    echo "Resolving $INPUT to SRR accessions..."
    esearch -db sra -query "$INPUT" | efetch -format runinfo | cut -d',' -f1 | grep SRR > "$ACCESSION_FILE"
    if [[ ! -s "$ACCESSION_FILE" ]]; then
      echo "Error: No SRR found for $INPUT"
      exit 1
    fi
  else
    echo "$INPUT" > "$ACCESSION_FILE"
  fi
fi

# ---- Process each accession ----
while read -r ACC; do
  echo "Processing $ACC"
  ACC_DIR="$FASTQ/$ACC"
  mkdir -p "$ACC_DIR"

  rm -f "$ACC_DIR/$ACC.sra.lock" 2>/dev/null || true

  prefetch "$ACC" --progress --output-directory "$ACC_DIR"

  SRA_FILE=$(find "$ACC_DIR" -name "*.sra" | head -n 1)
  if [[ -z "$SRA_FILE" || ! -f "$SRA_FILE" ]]; then
    echo "SRA file not found for $ACC in $ACC_DIR"
    continue
  fi

  fasterq-dump "$SRA_FILE" -O "$ACC_DIR" --temp "$ACC_DIR/tmp"

  FQ1="$ACC_DIR/${ACC}_1.fastq"
  FQ2="$ACC_DIR/${ACC}_2.fastq"
  FQ="$ACC_DIR/${ACC}.fastq"

  if [[ -f "$FQ1" && -f "$FQ2" ]]; then
    READ_LEN=$(awk 'NR==2 {print length($0)}' "$FQ1")
    kraken2 --db "$DB" --paired "$FQ1" "$FQ2" \
      --threads 4 --use-names \
      --report "$KRAKEN/${ACC}_report.txt" \
      --output "$KRAKEN/${ACC}_output.txt"
  elif [[ -f "$FQ" ]]; then
    READ_LEN=$(awk 'NR==2 {print length($0)}' "$FQ")
    kraken2 --db "$DB" "$FQ" \
      --threads 4 --use-names \
      --report "$KRAKEN/${ACC}_report.txt" \
      --output "$KRAKEN/${ACC}_output.txt"
  else
    echo "No FASTQ found for $ACC, skipping."
    continue
  fi

  bracken -d "$DB" \
    -i "$KRAKEN/${ACC}_report.txt" \
    -o "$BRACKEN/${ACC}_bracken.txt" \
    -l S -r "$READ_LEN"

  awk 'NR>1 {gsub(/ /, "_", $1); printf "%s_%s\t%s\n", $1, $2, $NF}' "$BRACKEN/${ACC}_bracken.txt" \
    | sort -k2,2nr | head -n "$TOP_N" > "$TMP/${ACC}_species.tsv"

  cut -f1 "$TMP/${ACC}_species.tsv" >> "$TMP/all_species.tsv"

done < "$ACCESSION_FILE"

# ---- Create CSV header ----
sort "$TMP/all_species.tsv" | uniq > "$TMP/species_headers.txt"

echo -n "sample" > "$CSV"
while read -r species; do
  echo -n ",$species" >> "$CSV"
done < "$TMP/species_headers.txt"
echo >> "$CSV"

# ---- Fill CSV matrix ----
while read -r ACC; do
  echo -n "$ACC" >> "$CSV"
  while read -r species; do
    value=$(awk -v sp="$species" -F"\t" '$1 == sp {print $2}' "$TMP/${ACC}_species.tsv")
    echo -n ",${value:-0}" >> "$CSV"
  done < "$TMP/species_headers.txt"
  echo >> "$CSV"
done < "$ACCESSION_FILE"

echo "Final output CSV: $CSV"

