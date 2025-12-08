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
RUN_REG_SECRET ?= ZWE0ZGU2YjUtMmFhZC00MmRmLTllZTgtZWY2ZDE2M2M5N2M5Ok56Y3lOREV3WkdNdE1XWXdPQzAwWTJGbUxUaGpZVEV0TkRCaFpHWTRPRGM1WXpoaQ==
RUN_SECP256K1_PRIVHEX ?= 4485fac780d28843da6292557053a41bf99ec70dba636a64240fa289fe12f62e
RUN_DMS_BASE_URL ?= https://dms.dev.aukiverse.com
RUN_DDS_BASE_URL ?= https://dds.dev.aukiverse.com
RUN_NODE_URL ?= https://9bfd6cf0f059.ngrok-free.app
RUN_NODE_VERSION ?= 0.0.0
RUN_REQUEST_TIMEOUT ?= 10
RUN_REGISTER_INTERVAL ?= 120
RUN_REGISTER_MAX_RETRY ?= -1
RUN_LOG_FORMAT ?= text

run:
	@if [ -z "$(RUN_REG_SECRET)" ]; then echo "error: set RUN_REG_SECRET to your DDS registration secret (export RUN_REG_SECRET=...)"; exit 1; fi
	@if [ -z "$(RUN_SECP256K1_PRIVHEX)" ]; then echo "error: set RUN_SECP256K1_PRIVHEX to the hex-encoded secp256k1 private key"; exit 1; fi
	RUST_LOG="debug" \
	POLL_BACKOFF_MS_MIN="10000" \
	ENABLE_NOOP=true \
	DMS_BASE_URL="$(RUN_DMS_BASE_URL)" \
	DDS_BASE_URL="$(RUN_DDS_BASE_URL)" \
	NODE_URL="$(RUN_NODE_URL)" \
	NODE_VERSION="$(RUN_NODE_VERSION)" \
	REQUEST_TIMEOUT_SECS="$(RUN_REQUEST_TIMEOUT)" \
	REGISTER_INTERVAL_SECS="$(RUN_REGISTER_INTERVAL)" \
	REGISTER_MAX_RETRY="$(RUN_REGISTER_MAX_RETRY)" \
	REG_SECRET="$(RUN_REG_SECRET)" \
	SECP256K1_PRIVHEX="$(RUN_SECP256K1_PRIVHEX)" \
	LOG_FORMAT="$(RUN_LOG_FORMAT)" \
	cargo run --package bin
