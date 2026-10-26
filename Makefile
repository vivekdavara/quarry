# `make check` is what every commit must pass: formatting, Clippy with warnings as errors, then every test.
.PHONY: check fmt lint test build clean

check: fmt lint test

fmt:
	cargo fmt --all -- --check

lint:
	cargo clippy --workspace --all-targets --locked -- -D warnings

test:
	cargo test --workspace --locked

build:
	cargo build --workspace --release --locked

clean:
	cargo clean
