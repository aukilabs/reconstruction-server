.PHONY: build test fmt clippy run

ci: fmt clippy test

build:
	cargo build --workspace

test:
	cargo test --workspace

fmt:
	cargo fmt --all

clippy:
	cargo clippy --all-targets --all-features -- -D warnings

run:
	cargo run -p server-bin -- --mock-python
