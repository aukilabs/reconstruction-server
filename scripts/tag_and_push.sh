#!/bin/bash

# Helper script to tag and push a (newly built) docker image to the docker hub.
# We add both a specific version tag and 'latest' when uploading, to ensure Akash can cache the image correctly.
# After building with docker compose up --build, find docker image id from "docker images"
# Then, run tag_and_push.sh [image_id] [tag]

# Check if both arguments are provided
if [ $# -ne 3 ]; then
    echo "Simple script to tag and upload a docker image as BOTH a custom tag and 'latest'."
    echo "Cloud servers like Akash cache images and should typically point to a specific tag rather than 'latest'."
    echo "Usage: $0 <image_id> <docker_hub_repo> <tag>"
    echo "Example: $0 IMAGE12345678 robinauki/refinement-server robin-dev-3"
    exit 1
fi

IMAGE_ID=$1
DOCKER_HUB_REPO=$2
TAG=$3

# Tag the image
docker tag $IMAGE_ID $DOCKER_HUB_REPO:$TAG
docker tag $IMAGE_ID $DOCKER_HUB_REPO:latest

# Push the images
docker push $DOCKER_HUB_REPO:$TAG
docker push $DOCKER_HUB_REPO:latest

echo "Image $IMAGE_ID tagged and pushed as $DOCKER_HUB_REPO:$TAG and $DOCKER_HUB_REPO:latest"