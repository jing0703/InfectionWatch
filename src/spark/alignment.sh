#!/bin/bash

#
# Mapping metagenomic data using Sparkhit algorithm.
#   - Lists the files in the raw sequence data bucket.
#   - Lists the files in the analyzed result bucket.
#   - Uses aws-cli to check unanalyzed files.
#   - Use Sparkhit algorithm to map raw sequence data to reference genome and save result to output bucket.
#   - Logs to STDOUT

# get file list as an array for raw sequence data bucket
input_list=(`aws s3 ls s3://split-seq | awk '{print $4}'`)
# get result file list as an array for output file bucket
output_dir=(`aws s3 ls s3://otu-result | awk '{print $2}'| tr -d /`)

function log {
  echo "[$(date --rfc-3339=seconds)]: $*"
}

# analysis raw sequence data with Sparkhit mapping algorithm and save result to output file  bucket
function mapping_seq {

    for file in ${input_list[@]}
    do 
        raw_seq="s3a://split-seq/$file"
        output_path="s3a://otu-result/${file:0:13}"
        master_node="spark://ec2-107-21-13-191.compute-1.amazonaws.com:7077"
        if [[ " ${output_dir[*]} " == *" ${file:0:13} "* ]]; then
            log "$file has been analyzed"
        else
            ./sparkhit-latest/bin/sparkhit mapper --master $master_node --driver-memory 6G --executor-memory 6G  -partition 320 -fastq $raw_seq -reference ./all_pathogen.fna -outfile $output_path
        fi
    done       
}

function ensure_only_running {
  if [ "$(pgrep -fn $0)" -ne "$(pgrep -fo $0)" ]; then
    log "Detected multiple instances of $0 running, exiting."
    exit 1
  fi
}

log "Starting to mapping sequence ($DATESTART)"
ensure_only_running
mapping_seq
echo "Finished."