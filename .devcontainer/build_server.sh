#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <reconstruction_version>" >&2
  echo "Example: $0 0.3.0" >&2
  exit 1
fi

export RECONSTRUCTION_VERSION="$1"
echo "[build_server] Using RECONSTRUCTION_VERSION=${RECONSTRUCTION_VERSION}"

cd /app/server/rust

echo "[build_server] Fetching Rust dependencies..."
cargo fetch

echo "[build_server] Building compute-node (release)..."
cargo build --release -p bin

echo "[build_server] Built /app/server/rust/target/release/compute-node"

cp /app/server/rust/target/release/compute-node /app/compute-node
chmod +x /app/compute-node

echo "[build_server] Done! Server binary: /app/compute-node"