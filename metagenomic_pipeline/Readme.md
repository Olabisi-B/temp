Welcome this is a pipeline that allows direct processing of metagenomic to taxonomic abundance profiles!

You can access this pipeline using python or script after cloning!
    - It requires sra-toolkit to run if entering accession numbers. By running setup.sh or environment.yml that downloads it on your device
if not previously installed.
    - You also need a kraken database, for our project we utilized the standard database. You can also download others from https://github.com/DerrickWood/kraken2/blob/master/README.md or https://benlangmead.github.io/aws-indexes/k2

Quickstart:
    - For the most ideal experience please make sure conda is installed!
    - git clone
    - cd metagenomic pipeline
    - conda env create -f environment.yml or bash setup.sh (optional) 
    - conda activate wm-igem
    - then run python pipeline.py or ./processing.sh -i <input (fastq|accession|file)> -o <output_dir> -d <kraken2_db_path> [-b bracken_threshold] [--keep-fastq]

1. If python is easier simply run the below command, then follow the prompts!:
    python search_sra.py or python3 search_sra.py 
2. Otherwise run ./processing.sh followed by the commands below
Commands: 
    i - input fastq or accession number or txt file with list of accession numbers
    o - output directory
    d - kraken database 
    b - input a bracken threshold
    --keep-fastq - this is toggled off but if you wnated to keep the fastq files after running, putting this saves the fastq file

Example: ./processing.sh -i accession.txt -d kraken2-db-8gb -o example_output
       
