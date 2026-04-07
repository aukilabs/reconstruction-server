#!/usr/bin/env bash
set -euo pipefail

# Clean build of server binary
sudo rm -rf build
mkdir -p build
cmake -B build -DCMAKE_BUILD_TYPE=Release -DPYBIND11_FINDPYTHON=ON
cmake --build build

# Clean up old build output first to avoid permission error
sudo rm -rf /app/server/rust/target/release
sudo rm -f /app/compute-node

bash .devcontainer/build_server.sh 0.0.0

if [ -f /app/.env ]; then
    echo ".env file found, configuring .bashrc to apply it on new shells..."
    echo 'source .devcontainer/apply_env_file.sh' >> ~/.bashrc
fi

echo "Done!"