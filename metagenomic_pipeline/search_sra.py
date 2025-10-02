import argparse
import subprocess
import os
import sys

def main():
    print("\n Welcome to the W&M IGEM 2025 Pipeline! \n")
    print("This pipeline allows you to process sequencing data using Kraken2 + Bracken.\n"
          "You can provide either:\n"
          "  - an SRA accession (e.g., SRR123456), which will be downloaded automatically using the sra-toolkit, OR\n"
          "  - a path to a local FASTQ file (e.g., sample.fastq or sample.fastq.gz).\n"
          "The output will include Bracken output, Kraken reports, and Kraken classification results for each sample.\n")

    # Prompt for input
    user_input = input("Enter SRA accession or FASTQ file path: ").strip()
    if not user_input:
        print("Input is required.")
        sys.exit(1)

    db_path = input("Enter path to Kraken2 database: ").strip()
    if not db_path:
        print("Kraken2 database path is required.")
        sys.exit(1)

    output_dir = input("Enter output directory [default: output]: ").strip()
    if not output_dir:
        output_dir = "output"

    bracken_threshold = input("Enter Bracken abundance threshold [default: 10]: ").strip()
    if not bracken_threshold:
        bracken_threshold = "10"

    keep_fastq = input("Keep FASTQ files after download? (y/n) [default: n]: ").strip().lower()
    keep_fastq_flag = []
    if keep_fastq in ["y", "yes"]:
        keep_fastq_flag = ["--keep-fastq"]

    # Locate the processing.sh script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    processing_path = os.path.join(script_dir, "processing.sh")

    if not os.path.isfile(processing_path):
        print(f"Error: processing.sh not found at {processing_path}")
        sys.exit(1)

    print(f"\n Running pipeline for: {user_input}\n")

    # Build and run command
    cmd = [
        processing_path,
        "-i", user_input,
        "-o", output_dir,
        "-d", db_path,
        "-b", bracken_threshold
    ] + keep_fastq_flag

    try:
        subprocess.run(cmd, check=True)
        print(f"\n Pipeline complete. Results saved in: {output_dir}")
    except subprocess.CalledProcessError as e:
        print(f"\n Error running pipeline: {e}")
        sys.exit(e.returncode)

if __name__ == "__main__":
    main()
