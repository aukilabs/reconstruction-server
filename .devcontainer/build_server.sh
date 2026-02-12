#!/usr/bin/env bash
set -euo pipefail

cd /app/server/rust

echo "[build_server] Fetching Rust dependencies..."
cargo fetch

echo "[build_server] Building compute-node (release)..."
cargo build --release -p bin

echo "[build_server] Built /app/server/rust/target/release/compute-node"

cp /app/server/rust/target/release/compute-node /app/compute-node
chmod +x /app/compute-node

echo "[build_server] Done! Server binary: /app/compute-node"