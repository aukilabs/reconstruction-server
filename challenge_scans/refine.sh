#!/bin/bash
domain_id=$1
scan_id=$2
clean_run=$3

if [ -z "$domain_id" ] || [ -z "$scan_id" ]; then
    echo "Usage: $0 <domain_id> <scan_id> [clean_run=false]"
    exit 1
fi

raw_scan_path=challenge_scans/$domain_id/$scan_id
job_path=jobs/challenge_scans/$domain_id/job_$scan_id
out_path=$job_path/refined/local
scan_path=$job_path/datasets/$scan_id

if [ ! -d "$raw_scan_path" ]; then
    echo "Raw scan path does not exist: $raw_scan_path"
    exit 1
fi

if [ "$clean_run" = "true" ] && [ -d "$job_path" ]; then
    rm -rf $job_path
fi

mkdir -p $out_path
if [ ! -d "$scan_path" ]; then
    mkdir -p $scan_path
    cp -r $raw_scan_path/* $scan_path
fi
python3 main.py --domain_id $domain_id --mode local_refinement --job_root_path $job_path --output_path $out_path --local_refinement_workers 0 --scans $scan_id