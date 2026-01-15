SHELL := /bin/bash

.PHONY: fmt clippy test ci
.PHONY: run

fmt:
	cargo fmt --all --check

clippy:
	cargo clippy --workspace --all-targets --all-features -- -D warnings

test:
	cargo test --workspace

ci: fmt clippy test
	@echo "CI checks passed."

# --- Local runtime helpers ---
RUN_REG_SECRET ?= your-registration-secret
RUN_SECP256K1_PRIVHEX ?= your-evm-private-key
RUN_NODE_URL ?= https://your-public-url

run:
	@if [ -z "$(RUN_REG_SECRET)" ]; then echo "error: set RUN_REG_SECRET to your DDS registration secret (export RUN_REG_SECRET=...)"; exit 1; fi
	@if [ -z "$(RUN_SECP256K1_PRIVHEX)" ]; then echo "error: set RUN_SECP256K1_PRIVHEX to the hex-encoded secp256k1 private key"; exit 1; fi
	RUST_LOG="debug" \
	NODE_URL="$(RUN_NODE_URL)" \
	REG_SECRET="$(RUN_REG_SECRET)" \
	SECP256K1_PRIVHEX="$(RUN_SECP256K1_PRIVHEX)" \
	cargo run --package bin
