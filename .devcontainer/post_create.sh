#!/usr/bin/env bash
set -euo pipefail

mkdir -p build
cmake -B build -DCMAKE_BUILD_TYPE=Release -DPYBIND11_FINDPYTHON=ON
cmake --build build

# Clean up old build first to avoid permission error
sudo rm /app/server/rust/target/release/compute-node*
sudo rm /app/compute-node

bash .devcontainer/build_server.sh

echo "Done!"