import pandas as pd
import re
import os
import pyarrow as pa
import pyarrow.parquet as pq

# Input and output file names
csv_file = "all_merged.csv"
parquet_file = "AQUERY.parquet"
parquet_long = "AQUERY_long.parquet"

# Function to clean column names for safe SQL usage
def clean_name(name: str) -> str:
    # Normalize whitespace
    name = re.sub(r"\s+", "_", name.strip().lower())
    # Remove quotes
    name = name.replace("'", "").replace('"', "")
    # Replace unsafe characters with underscore
    name = re.sub(r"[^0-9a-zA-Z_]", "_", name)
    # Remove leading/trailing underscores
    return name.strip("_")

# --- Load or create WIDE parquet ---
if not os.path.exists(parquet_file):
    print("Converting CSV → Parquet (this may take a while)...")
    df = pd.read_csv(csv_file, low_memory=False, dtype=str)  # load everything as string
    df = df.rename(columns=lambda c: clean_name(str(c)))     # normalize column names
    df.to_parquet(parquet_file, engine="pyarrow", index=False)
    print("Wide-format Parquet created:", parquet_file)
else:
    print("Wide-format Parquet already exists, loading...")
    df = pd.read_parquet(parquet_file, engine="pyarrow")

# --- Columns to keep as metadata (not species) ---
metadata_cols = [
    "accession", "latitude", "longitude", "environmental_condition", "season",
    "depth", "temperature", "salinity", "ph", "carbon", "phosphorus",
    "carbon_dioxide", "organic_carbon", "inorganic_carbon", "nitrate", "nitrite",
    "nitrogen", "oxygen_concentration", "phosphate", "chlorophyll", "chloride",
    "methane", "date"
]
metadata_cols = [c for c in metadata_cols if c in df.columns]

# Candidate species columns = everything else
candidate_cols = [c for c in df.columns if c not in metadata_cols]

# Keep only columns that look numeric (likely abundance values)
species_cols = []
for c in candidate_cols:
    try:
        pd.to_numeric(df[c].dropna().head(50))  # quick sample check
        species_cols.append(c)
    except Exception:
        print(f"Skipping non-numeric column: {c}")

# --- Convert to LONG format (streaming batches, no melt) ---
if not os.path.exists(parquet_long):
    print("Converting to long format in batches (numeric only, excluding metadata/env cols)...")

    batch_size = 200
    writer = None

    for i in range(0, len(species_cols), batch_size):
        subset = species_cols[i:i+batch_size]
        print(f"  Processing batch {i}–{i+len(subset)} of {len(species_cols)} species")

        records = []
        for sp in subset:
            tmp = df[metadata_cols].copy()
            tmp["species"] = sp
            tmp["abundance"] = pd.to_numeric(df[sp], errors="coerce").fillna(0)  # force numeric, replace NaN with 0
            records.append(tmp)

        batch_long = pd.concat(records, ignore_index=True)

        # Convert to Arrow table
        table = pa.Table.from_pandas(batch_long, preserve_index=False)

        if writer is None:
            writer = pq.ParquetWriter(parquet_long, table.schema)

        writer.write_table(table)

    if writer:
        writer.close()

    print("Long-format Parquet created:", parquet_long)
else:
    print("Long-format Parquet already exists, skipping.")
