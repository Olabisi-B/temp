#!/bin/bash
set -euo pipefail

# ---- Inputs ----
OUTDIR="${1:-output}"
TMP="$OUTDIR/tmp"
WIDE_OUT="${2:-$OUTDIR/abundance_matrix.csv}"
LONG_OUT="${3:-$OUTDIR/abundance_long.tsv}"

mkdir -p "$(dirname "$WIDE_OUT")"
mkdir -p "$(dirname "$LONG_OUT")"

if ! compgen -G "$TMP"/*_species.tsv > /dev/null; then
  echo "[ERROR] No *_species.tsv files found in $TMP"
  exit 1
fi

echo "[STEP] Reading Bracken species files from: $TMP"

declare -A matrix
declare -A species_set
declare -A accessions
declare -A sample_sums

# ---- Read and process files ----
for file in "$TMP"/*_species.tsv; do
  acc=$(basename "$file" | cut -d'_' -f1)
  accessions["$acc"]=1
  sample_sum=0

  while IFS=$'\t' read -r genus species abundance; do
    sp="${genus// /_}_${species// /_}"
    species_set["$sp"]=1
    matrix["$acc,$sp"]="$abundance"
    sample_sum=$(echo "$sample_sum + $abundance" | bc)
  done < "$file"

  sample_sums["$acc"]="$sample_sum"
done

IFS=$'\n' species_list_sorted=($(sort <<<"${!species_set[*]}"))
IFS=$'\n' accession_list_sorted=($(sort <<<"${!accessions[*]}"))

# ---- Write wide format CSV ----
{
  printf "Accession"
  for sp in "${species_list_sorted[@]}"; do
    printf ",%s" "$sp"
  done
  printf "\n"

  for acc in "${accession_list_sorted[@]}"; do
    total="${sample_sums[$acc]}"
    printf "%s" "$acc"
    for sp in "${species_list_sorted[@]}"; do
      raw="${matrix["$acc,$sp"]:-0}"
      norm=$(echo "scale=6; $raw / $total" | bc)
      printf ",%s" "$norm"
    done
    printf "\n"
  done
} > "$WIDE_OUT"

echo "[DONE] Wrote normalized wide matrix to: $WIDE_OUT"

# ---- Write long format TSV ----
{
  printf "Accession\tSpecies\tAbundance\n"
  for acc in "${accession_list_sorted[@]}"; do
    total="${sample_sums[$acc]}"
    for sp in "${species_list_sorted[@]}"; do
      raw="${matrix["$acc,$sp"]:-0}"
      norm=$(echo "scale=6; $raw / $total" | bc)
      printf "%s\t%s\t%s\n" "$acc" "$sp" "$norm"
    done
  done
} > "$LONG_OUT"

echo "[DONE] Wrote long-format normalized table to: $LONG_OUT"

