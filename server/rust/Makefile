build:
	cargo build --workspace

test:
	cargo test --workspace

fmt:
	cargo fmt --all

clippy:
	cargo clippy --all-targets --all-features -- -D warnings

run:
	cargo run -p server-bin -- --api-key secret --port :8080

# Run server with mock DDS env configured
run-with-dds:
	DDS_BASE_URL=https://dds.dev.aukiverse.com \
	NODE_URL=https://6e2b85009ad2.ngrok-free.app \
	NODE_VERSION=1.0.0 \
	REG_SECRET=<get_from_console> \
	SECP256K1_PRIVHEX=<stake_private_key> \
	cargo run -p server-bin -- --api-key secret --port :8080
