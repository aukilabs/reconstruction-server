#!/bin/bash

# Specify the known directory
KNOWN_DIR="/path/to/jobs/my_domain_job/datasets/"
COMMENT_PREFIX="dmt_scan"

# Get a list of all sub-directories
SUB_DIRS=$(find "$KNOWN_DIR" -mindepth 1 -maxdepth 1 -type d)

# Loop through each sub-directory and run your Python script
for dir in $SUB_DIRS; do
    last_part=$(basename "$dir")
    if [[ "$last_part" == "$COMMENT_PREFIX"* ]]; then
        echo "Working on directory: $dir"
        # python local_main.py --dataset_path "$dir" --remove_outputs
        docker run \
        --gpus all \
        --shm-size=512m \
        -v /path/to/jobs/:/path/to/jobs/ \
        -it \
        auki-archive:latest \
        local_main \
        --dataset_path "$dir" \
        --output_path /path/to/jobs/my_domain_job/refined/local/ \
        --every_nth_image 2 
    fi
done