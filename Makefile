.PHONY: build release proto test clean fmt lint

# Default: build debug binary
build:
	cargo build

# Optimized release binary (small, fast, stripped)
release:
	cargo build --release

# Regenerate protobuf Rust code (happens automatically via build.rs,
# but this target forces a rebuild)
proto:
	cargo build --build-plan > /dev/null 2>&1 || true
	@echo "Protobuf code is generated automatically by build.rs during cargo build"

# Run tests
test:
	cargo test

# Format code
fmt:
	cargo fmt

# Lint
lint:
	cargo clippy -- -D warnings

# Remove build artifacts
clean:
	cargo clean
