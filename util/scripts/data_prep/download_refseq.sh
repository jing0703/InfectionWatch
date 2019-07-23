#! /bin/sh

'''download the most recent complete bacterial genomes as one fasta file'''

#Get the list of assemblies
wget ftp://ftp.ncbi.nlm.nih.gov/genomes/genbank/bacteria/assembly_summary.txt
#Parse the addresses of complete genomes from it (right now n = 4,804):
awk -F '\t' '{if($12=="Complete Genome") print $20}' assembly_summary.txt > assembly_summary_complete_genomes.txt
#Make a dir for data
mkdir GbBac
#Fetch data
for next in $(cat assembly_summary_complete_genomes.txt); do wget -P GbBac "$next"/*genomic.fna.gz; done
#Extract data
find ./GbBac -name '*.gz' -exec gunzip {} +
#Concatenate data
cat GbBac/*.fna > all_complete_Gb_bac.fasta