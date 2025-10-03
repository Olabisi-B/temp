This folder metaG_metaT/ is a space for testing differences between a metagenomic and metatransciptomic sample from a singular study/sample location 
to review variances in the taxonomy found using DNA vs RNA. This a good thing to consider going forward in chassis selection and highlights the value in doing
both metagenomic and metatransciptomic sequencing 

Prerequisites
- Kraken2 and Kraken database
- Bracken
- If running using sra accession numbers, sra toolkit should be pre installed

How to run:
1. git clone
2. depending on user preference either process_both.sh or python_loader.py may be ran
    - ./parser.sh
    - python similar.py or python3 similar.py
3. Enter path to accesssion.txt file with accession numbers for metag and metat (respectively and comma seperated) or fastq files, output directory, top n and bracken_threshold. 