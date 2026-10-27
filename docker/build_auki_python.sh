#!/usr/bin/env bash
# Build the Auki Python packages the pipeline imports into wheels:
# - the Auki SDK bindings, at the auki-sdk rev server/rust/Cargo.toml pins, so the Python and Rust
#   sides use one SDK version;
# - qr-lab and pnp-lab (QR detection and pose estimation for Auki sessions), at the revs below.
#   build_auki_python.sh <repository root> <wheel output dir>
# Needs git access to github.com/aukilabs/auki-sdk (private), a Rust toolchain, and the Python the
# wheels are for (qr-lab and pnp-lab build for one Python version). Uses maturin from PATH, or
# installs a pinned one with cargo.
set -euo pipefail

QR_LAB_REV=c99d6ca46657c0995b3a323138794f56db549a9e
PNP_LAB_REV=f9b27235d6a39ff8232ffb79d0bbf179f8e39085

repo_root="$(cd "$1" && pwd)"
out_dir="$(mkdir -p "$2" && cd "$2" && pwd)"

sdk_rev="$(sed -n 's/^auki-sdk = .*rev = "\([0-9a-f]*\)".*/\1/p' "$repo_root/server/rust/Cargo.toml")"
if [ -z "$sdk_rev" ]; then
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

checkout() {  # checkout <name> <repo url> <rev>
    git -C "$work" init -q "$1"
    git -C "$work/$1" fetch -q --depth 1 "$2" "$3"
    git -C "$work/$1" checkout -q FETCH_HEAD
}

build() {  # build <package dir>
    (cd "$1" && CARGO_NET_GIT_FETCH_WITH_CLI=true "$maturin" build --release --locked -i python3 -o "$out_dir")
}

checkout auki-sdk https://github.com/aukilabs/auki-sdk "$sdk_rev"
for package in auki-geometry-py auki-registry-py auki-layout-py auki-logs-py; do
    build "$work/auki-sdk/labs/bindings/python/$package"
done
# Pure Python (betterproto messages), so pip builds it.
python3 -m pip wheel --quiet --no-deps -w "$out_dir" "$work/auki-sdk/labs/bindings/python/auki-datatypes-py"

checkout qr-lab https://github.com/aukilabs/qr-lab "$QR_LAB_REV"
build "$work/qr-lab/bindings/python"
checkout pnp-lab https://github.com/aukilabs/pnp-lab "$PNP_LAB_REV"
build "$work/pnp-lab/bindings/python"

echo "built into $out_dir: auki-sdk $sdk_rev, qr-lab $QR_LAB_REV, pnp-lab $PNP_LAB_REV"
