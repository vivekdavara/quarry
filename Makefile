# `make check` is what every commit must pass: formatting, Clippy with warnings as errors, every test, and the docs
# with no broken links.
.PHONY: check fmt lint test doc build clean

check: fmt lint test doc

fmt:
	cargo fmt --all -- --check

lint:
	cargo clippy --workspace --all-targets --locked -- -D warnings

test:
	cargo test --workspace --locked

doc:
	RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked

build:
	cargo build --workspace --release --locked

clean:
	cargo clean
