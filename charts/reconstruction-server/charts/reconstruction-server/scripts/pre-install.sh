#!/usr/bin/env bash

# Set to dummy image to make tests pass as the real image isn't public yet

yq -i '(.image.repository = "registry.k8s.io/pause") | (.image.tag = "3.10")' "$(dirname "$0")/../values.yaml"
