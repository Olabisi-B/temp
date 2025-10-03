import os
import glob
import sys
import subprocess
import pandas as pd
from scipy.spatial.distance import cosine

def run_command(cmd):
    """Run a shell command and raise error if it fails."""
    print(f"\n[CMD] {cmd}")
    try:
        # Using subprocess.run for better error handling and output visibility
        subprocess.run(cmd, shell=True, check=True, executable="/bin/bash")
    except subprocess.CalledProcessError as e:
        print(f"\n--- ERROR: Command failed with exit code {e.returncode} ---")
        print(f"Failed command: {e.cmd}")
        sys.exit(1)


def parse_classification_percents(report_file):
    """
    Parse Kraken2 report file to get %classified and %unclassified.
    """
    percent_classified = 0.0
    percent_unclassified = 0.0
    with open(report_file) as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) < 2:
                continue
            pct = float(parts[0].strip())
            name = parts[-1].strip()
            
            if name == "unclassified":
                percent_unclassified = pct
            elif name == "root":
                percent_classified = pct  # Use root for total classified
    
    if percent_classified == 0.0 and percent_unclassified == 0.0:
        print("[WARNING] Could not parse classification percentages cleanly. Defaulting to 0.")
    
    return percent_classified, percent_unclassified


def prepare_input(input_source, output_folder):
    """
    Handles SRA download or local FASTQ setup.
    Returns: list of FASTQ files, and the canonical ID (accession or filename).
    """
    if os.path.isfile(input_source):
        if input_source.endswith(".fastq") or input_source.endswith(".fastq.gz"):
            print(f"[INFO] Detected local FASTQ: {input_source}")
            # Canonical ID is the base filename without extension
            base_name = os.path.basename(input_source).split('.')[0]
            return [input_source], base_name
        else:
            raise FileNotFoundError(f"Input file {input_source} does not look like a FASTQ. Please use .fastq or .fastq.gz.")
    
    else:
        # Case 2: Assume SRA accession
        acc = input_source
        print(f"[INFO] Detected SRA Accession: {acc}. Downloading and dumping...")
        
        sra_dir = os.path.join(output_folder, acc)
        os.makedirs(sra_dir, exist_ok=True)

        # Prefetch the SRA file
        run_command(f"prefetch {acc} --output-directory {sra_dir} --max-size 500G")
        
        tmp_dump_dir = os.path.join(sra_dir, "tmp_dump")
        os.makedirs(tmp_dump_dir, exist_ok=True)
        run_command(f"fasterq-dump {acc} -O {sra_dir} --temp {tmp_dump_dir} --split-files")
        
        # Find the resulting FASTQ files
        fastq_files = glob.glob(f"{sra_dir}/{acc}*.fastq")
        
        if not fastq_files:
            raise RuntimeError(f"fasterq-dump failed to produce FASTQ files for {acc}.")
            
        return fastq_files, acc


def run_kraken_bracken(fastq_files, kraken_db, outdir, prefix, read_len):
    """Run Kraken2 + Bracken, return path to Bracken CSV and classification %s."""
    
    print(f"\n--- Running Kraken2 for {prefix} ---")
    
    kraken_out = os.path.join(outdir, f"{prefix}_kraken2.out")
    kraken_report = os.path.join(outdir, f"{prefix}_kraken2.report")
    bracken_tsv = os.path.join(outdir, f"{prefix}_bracken.tsv")
    bracken_csv = os.path.join(outdir, f"{prefix}_bracken.csv")

    fq_inputs = " ".join(fastq_files)
    
    kraken_paired_flag = "--paired" if len(fastq_files) > 1 else ""

    run_command(
        f"kraken2 --db {kraken_db} --threads 8 --use-names "
        f"{kraken_paired_flag} {fq_inputs} "
        f"--report {kraken_report} --output {kraken_out}"
    )
    
    print(f"\n--- Running Bracken for {prefix} (Read Length: {read_len}) ---")
    
    run_command(
        f"bracken -d {kraken_db} -i {kraken_report} -o {bracken_tsv} -l S -r {read_len}"
    )

    try:
        df = pd.read_csv(bracken_tsv, sep="\t")
        df.to_csv(bracken_csv, index=False)
        os.remove(bracken_tsv)
    except Exception as e:
        print(f"[ERROR] Failed to convert Bracken TSV to CSV: {e}")
        sys.exit(1)

    # Classification percentages
    pct_class, pct_unclass = parse_classification_percents(kraken_report)

    return bracken_csv, pct_class, pct_unclass


def merge_metag_metat(bracken_g_csv, bracken_t_csv,
                      pct_g_class, pct_g_unclass,
                      pct_t_class, pct_t_unclass,
                      merged_csv, overlap_csv):
    """
    Merge two Bracken CSVs (MetaG + MetaT) by species name, adding difference and classification stats.
    """
    print("\n--- Merging results using pandas ---")

    # Load dataframes and add suffixes
    df_g = pd.read_csv(bracken_g_csv).add_suffix("_metaG")
    df_t = pd.read_csv(bracken_t_csv).add_suffix("_metaT")

    # Normalize species column name for merging
    df_g = df_g.rename(columns={"name_metaG": "name"})
    df_t = df_t.rename(columns={"name_metaT": "name"})
    
    required_cols = ['name', 'fraction_total_reads_metaG', 'fraction_total_reads_metaT']

    # --- 1. MERGED (Full Outer Join) ---
    merged = pd.merge(df_g, df_t, on="name", how="outer", suffixes=("_metaG", "_metaT"))
    
    # Fill NaN fractions with 0 for calculation
    merged["fraction_total_reads_metaG"] = merged["fraction_total_reads_metaG"].fillna(0)
    merged["fraction_total_reads_metaT"] = merged["fraction_total_reads_metaT"].fillna(0)

    # Calculate difference
    merged["difference"] = merged["fraction_total_reads_metaG"] - merged["fraction_total_reads_metaT"]

    # Fill remaining NaN values (for TaxID, reads, etc.) with appropriate placeholders
    merged = merged.fillna({"taxonomy_id_metaG": 0, "taxonomy_lvl_metaG": "S", 
                            "kraken_assigned_reads_metaG": 0, "added_reads_metaG": 0, "new_est_reads_metaG": 0, 
                            "taxonomy_id_metaT": 0, "taxonomy_lvl_metaT": "S", 
                            "kraken_assigned_reads_metaT": 0, "added_reads_metaT": 0, "new_est_reads_metaT": 0})
    
    # Reorder columns to match the required 15-column format
    output_cols = [
        'name',
        'taxonomy_id_metaG', 'taxonomy_lvl_metaG', 'kraken_assigned_reads_metaG', 'added_reads_metaG', 'new_est_reads_metaG', 'fraction_total_reads_metaG',
        'taxonomy_id_metaT', 'taxonomy_lvl_metaT', 'kraken_assigned_reads_metaT', 'added_reads_metaT', 'new_est_reads_metaT', 'fraction_total_reads_metaT',
        'difference'
    ]
    merged = merged[output_cols]

    # Add classification % as the first row metadata
    meta_row = pd.DataFrame([{
        "name": "CLASSIFICATION_STATS",
        "fraction_total_reads_metaG": pct_g_class,
        "new_est_reads_metaG": pct_g_unclass, # Using this field to store unclassified for clarity
        "fraction_total_reads_metaT": pct_t_class,
        "new_est_reads_metaT": pct_t_unclass, # Using this field to store unclassified for clarity
        "difference": pct_g_class - pct_t_class,
        # Set other columns to 0 or 'N/A' to ensure schema consistency
        "taxonomy_id_metaG": 0, "taxonomy_lvl_metaG": "N/A", "kraken_assigned_reads_metaG": 0, "added_reads_metaG": 0, 
        "taxonomy_id_metaT": 0, "taxonomy_lvl_metaT": "N/A", "kraken_assigned_reads_metaT": 0, "added_reads_metaT": 0
    }])
    
    merged = pd.concat([meta_row[output_cols], merged], ignore_index=True)
    merged.to_csv(merged_csv, index=False)

    # --- 2. OVERLAP (Inner Join) ---
    overlap = pd.merge(df_g, df_t, on="name", how="inner", suffixes=("_metaG", "_metaT"))
    
    overlap["difference"] = overlap["fraction_total_reads_metaG"] - overlap["fraction_total_reads_metaT"]

    # Reorder columns for overlap
    overlap = overlap[output_cols]
    
    # Add classification % as the first row metadata for overlap
    overlap = pd.concat([meta_row[output_cols], overlap], ignore_index=True)
    overlap.to_csv(overlap_csv, index=False)

    print(f"[INFO] Merged (Full Join) output written to: {merged_csv}")
    print(f"[INFO] Overlap (Inner Join) output written to: {overlap_csv}")
    return merged_csv, overlap_csv


def compute_similarity(bracken_g, bracken_t, folder_path, final_name):
    """Compute cosine similarity + difference score between two Bracken CSVs."""
    print("\n--- Computing Similarity Metrics ---")

    # Load the merged data for similarity computation
    df = pd.read_csv(bracken_g)
    
    # Exclude the CLASSIFICATION_STATS row if present
    df_g = df[df['name'] != 'CLASSIFICATION_STATS']
    df_t = pd.read_csv(bracken_t)
    df_t = df_t[df_t['name'] != 'CLASSIFICATION_STATS']

    # Normalize species column names before aligning
    df_g = df_g.rename(columns={"name": "name_metaG", "fraction_total_reads": "fraction_total_reads_metaG"})
    df_t = df_t.rename(columns={"name": "name_metaT", "fraction_total_reads": "fraction_total_reads_metaT"})

    # Perform outer merge to align species vectors, filling missing fractions with 0
    all_abund = pd.merge(df_g[['name_metaG', 'fraction_total_reads_metaG']],
                         df_t[['name_metaT', 'fraction_total_reads_metaT']],
                         left_on='name_metaG', right_on='name_metaT', how='outer')
    
    vec_g = all_abund['fraction_total_reads_metaG'].fillna(0).values
    vec_t = all_abund['fraction_total_reads_metaT'].fillna(0).values

    try:
        # Cosine similarity is 1 - distance
        sim = 1 - cosine(vec_g, vec_t)
    except Exception:
        sim = 0.0

    # L1 distance (Manhattan distance) or simple absolute difference sum
    difference_score = sum(abs(a - b) for a, b in zip(vec_g, vec_t))
    
    score_log = os.path.join(folder_path, f"{final_name}_similarity.log")
    with open(score_log, "w") as f:
        f.write(f"Metagenomic ID: {final_name.split('_')[0]}\n")
        f.write(f"Metatranscriptomic ID: {final_name.split('_')[1]}\n")
        f.write(f"Cosine similarity: {sim:.4f}\n")
        f.write(f"Difference score (Sum of Absolute Differences): {difference_score:.4f}\n")
        
    print(f"[INFO] Similarity metrics saved to {score_log}")
    return sim, difference_score

# --- Core Logic Function (renamed from main) ---
def execute_pipeline_logic(metaG_input, metaT_input, krakendb, outdir, read_len):
    """Executes the core pipeline steps using provided inputs."""
    
    # Check if read_len is valid here
    if not read_len.isdigit():
        print("Bracken read length must be an integer.")
        sys.exit(1)

    # 1. Setup Output Directory
    # Determine accession IDs for output naming and setup folders
    temp_metaG_name = os.path.basename(metaG_input).split('.')[0] if ('/' in metaG_input or '.' in metaG_input) else metaG_input
    temp_metaT_name = os.path.basename(metaT_input).split('.')[0] if ('/' in metaT_input or '.' in metaT_input) else metaT_input
    
    folder_name = f"{temp_metaG_name}_{temp_metaT_name}"
    folder_path = os.path.join(outdir, folder_name)
    os.makedirs(folder_path, exist_ok=True)
    
    print(f"\n[INFO] All analysis files will be placed in: {folder_path}")

    # 2. Prepare Inputs (Download SRA or use local files)
    try:
        metaG_files, metaG_acc_name = prepare_input(metaG_input, folder_path)
        metaT_files, metaT_acc_name = prepare_input(metaT_input, folder_path)
    except Exception as e:
        print(f"[FATAL] Input preparation failed: {e}")
        sys.exit(1)
        
    # Final name structure for the output files
    final_name = f"{metaG_acc_name}_{metaT_acc_name}"

    # 3. Run Kraken + Bracken
    try:
        metaG_bracken, pct_g_class, pct_g_unclass = run_kraken_bracken(
            metaG_files, krakendb, folder_path, f"{metaG_acc_name}_metaG", read_len
        )
        metaT_bracken, pct_t_class, pct_t_unclass = run_kraken_bracken(
            metaT_files, krakendb, folder_path, f"{metaT_acc_name}_metaT", read_len
        )
    except Exception as e:
        print(f"[FATAL] Kraken/Bracken execution failed: {e}")
        sys.exit(1)

    # 4. Merge results
    merged_file = os.path.join(folder_path, f"{final_name}_merged.csv")
    overlap_file = os.path.join(folder_path, f"{final_name}_overlap.csv")
    
    merge_metag_metat(metaG_bracken, metaT_bracken,
                      pct_g_class, pct_g_unclass,
                      pct_t_class, pct_t_unclass,
                      merged_file, overlap_file)

    # 5. Compute similarity
    compute_similarity(metaG_bracken, metaT_bracken, folder_path, final_name)

    print(f"\n[DONE] Pipeline successfully completed for {final_name}!")
    print(f"Results are in the folder: {folder_path}")




def main():
    print("\n Welcome to the W&M IGEM 2025 Full Analysis Pipeline (Interactive Edition)! \n")
    print("Please provide the required paths and IDs.")

    metaG_input = input("Enter metagenomic SRA accession or FASTQ file path (-g): ").strip()
    if not metaG_input:
        print("Metagenomic input is required.")
        sys.exit(1)

    metaT_input = input("Enter metatranscriptomic SRA accession or FASTQ file path (-t): ").strip()
    if not metaT_input:
        print("Metatranscriptomic input is required.")
        sys.exit(1)

    krakendb = input("Enter path to Kraken2 database (-db): ").strip()
    if not krakendb:
        print("Kraken2 database path is required.")
        sys.exit(1)

    outdir = input("Enter output directory [default: results] (-o): ").strip() or "results"
    
    read_len = input("Enter read length for Bracken [default: 150] (-r): ").strip() or "150"

    execute_pipeline_logic(metaG_input, metaT_input, krakendb, outdir, read_len)


if __name__ == "__main__":
    main()
