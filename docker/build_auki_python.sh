#!/usr/bin/env bash
# Build the Auki SDK Python bindings the pipeline imports into wheels, at the auki-sdk rev
# server/rust/Cargo.toml pins, so the Python and Rust sides use one SDK version.
#   build_auki_python.sh <repository root> <wheel output dir>
# Needs git access to github.com/aukilabs/auki-sdk and a Rust toolchain. Uses maturin from PATH,
# or installs a pinned one with cargo.
set -euo pipefail

repo_root="$(cd "$1" && pwd)"
out_dir="$(mkdir -p "$2" && cd "$2" && pwd)"
packages=(auki-geometry-py)

rev="$(sed -n 's/^auki-sdk = .*rev = "\([0-9a-f]*\)".*/\1/p' "$repo_root/server/rust/Cargo.toml")"
if [ -z "$rev" ]; then
    echo "no auki-sdk rev found in server/rust/Cargo.toml" >&2
    exit 1
fi

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

maturin="$(command -v maturin || true)"
if [ -z "$maturin" ]; then
    cargo install --quiet --locked --root "$work/tools" maturin --version 1.15.0
    maturin="$work/tools/bin/maturin"
fi

git -C "$work" init -q auki-sdk
git -C "$work/auki-sdk" fetch -q --depth 1 https://github.com/aukilabs/auki-sdk "$rev"
git -C "$work/auki-sdk" checkout -q FETCH_HEAD

for package in "${packages[@]}"; do
    (cd "$work/auki-sdk/labs/bindings/python/$package" &&
        CARGO_NET_GIT_FETCH_WITH_CLI=true "$maturin" build --release --locked -i python3 -o "$out_dir")
done
echo "auki-sdk $rev: built ${packages[*]} into $out_dir"
