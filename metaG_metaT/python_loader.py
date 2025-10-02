import subprocess
import os
import sys

def main():
    print("\n Welcome to the W&M IGEM 2025 Pipeline! \n")
    print("This pipeline processes BOTH genomic and transcriptomic sequencing data using Kraken2 + Bracken.\n"
          "You can provide either:\n"
          "  - an SRA accession (e.g., SRR123456), OR\n"
          "  - a path to a local FASTQ file (e.g., sample.fastq or sample.fastq.gz).\n")

    # Prompt for genomic input
    genomic_input = input("Enter genomic SRA accession or FASTQ file path: ").strip()
    if not genomic_input:
        print("Genomic input is required.")
        sys.exit(1)

    # Prompt for transcriptomic input
    transcript_input = input("Enter transcriptomic SRA accession or FASTQ file path: ").strip()
    if not transcript_input:
        print("Transcriptomic input is required.")
        sys.exit(1)

    db_path = input("Enter path to Kraken2 database: ").strip()
    if not db_path:
        print("Kraken2 database path is required.")
        sys.exit(1)

    output_dir = input("Enter output directory [default: output]: ").strip()
    if not output_dir:
        output_dir = "output"

    top_n = input("Enter number of top species to include [default: 500]: ").strip()
    if not top_n:
        top_n = "500"

    bracken_threshold = input("Enter Bracken abundance threshold [default: 10]: ").strip()
    if not bracken_threshold:
        bracken_threshold = "10"

    # Locate the processing.sh script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    processing_path = os.path.join(script_dir, "processing.sh")

    if not os.path.isfile(processing_path):
        print(f"Error: processing.sh not found at {processing_path}")
        sys.exit(1)

    print(f"\n Running pipeline for:\n"
          f"  Genomic input: {genomic_input}\n"
          f"  Transcriptomic input: {transcript_input}\n")

    # Build and run command
    cmd = [
        processing_path,
        "-g", genomic_input,
        "-r", transcript_input,
        "-o", output_dir,
        "-d", db_path,
        "-t", top_n,
        "-b", bracken_threshold
    ]

    try:
        subprocess.run(cmd, check=True)
        print(f"\n Pipeline complete.\n"
              f"Genomic results: {os.path.join(output_dir, 'genomic/genomic_abundance.csv')}\n"
              f"Transcriptomic results: {os.path.join(output_dir, 'transcriptomic/transcriptomic_abundance.csv')}\n"
              f"Similarity review printed at the end of the run.\n")
    except subprocess.CalledProcessError as e:
        print(f"\n Error running pipeline: {e}")
        sys.exit(e.returncode)

if __name__ == "__main__":
    main()
