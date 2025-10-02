#!/bin/bash
set -euo pipefail

INPUT=${1:-accessions_priority.txt}
OUTPUT=${2:-accession_to_srr.tsv}
LOG="unresolved_accessions.log"

export PATH="$HOME/edirect:$PATH"
export NCBI_API_KEY="525afbdc9a9012ce888748aab2eefc016409"
NCBI_EMAIL="obbashorun@wm.edu"

> "$OUTPUT"
> "$LOG"

echo "[INFO] Resolving accessions from $INPUT..."

while IFS= read -r ACCESSION || [[ -n "$ACCESSION" ]]; do
  ACCESSION="${ACCESSION//$'\r'/}"  # Strip carriage returns
  [[ -z "$ACCESSION" ]] && continue

  if [[ "$ACCESSION" =~ ^SRR[0-9]+$ ]]; then
    echo -e "$ACCESSION\t$ACCESSION" >> "$OUTPUT"
    echo "[OK] $ACCESSION is already an SRR"
    continue
  fi

  echo "[...] Resolving $ACCESSION to SRR..."

  SRR=""
  for attempt in {1..5}; do
    SRR=$(esearch -db biosample -query "$ACCESSION" | \
          elink -target sra | \
          efetch -format runinfo | \
          cut -d',' -f1 | grep ^SRR | head -n 1 || true)

    if [[ -n "$SRR" ]]; then
      echo -e "$ACCESSION\t$SRR" >> "$OUTPUT"
      echo "[OK] $ACCESSION → $SRR"
      break
    else
      echo "[WARN] Attempt $attempt failed for $ACCESSION. Retrying..."
      sleep $((RANDOM % 4 + 3))
    fi
  done

  if [[ -z "$SRR" ]]; then
    echo "[FAIL] Could not resolve $ACCESSION" | tee -a "$LOG"
  fi
done < "$INPUT"

echo "[DONE] Output written to $OUTPUT"

