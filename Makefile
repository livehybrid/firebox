# Firebox developer entry points. `make help` lists targets.
.DEFAULT_GOAL := help
SHELL := /bin/bash

TARGET   ?= x86_64-unknown-linux-musl
FEATURES ?= http
BIN      := target/$(TARGET)/release/firebox

.PHONY: help build build-tls test lint fmt bench clean dist

help: ## Show this help
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "} {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

build: ## Release build for $(TARGET) with $(FEATURES) (no TLS; needs no C toolchain)
	cargo build --release --target $(TARGET) --no-default-features --features "$(FEATURES)"
	@ls -la $(BIN)

build-tls: ## Release build with HTTPS support (needs a C compiler for ring)
	cargo build --release --target $(TARGET) --features tls

test: ## Unit + integration tests
	cargo test --target $(TARGET) --no-default-features --features "$(FEATURES)"

lint: ## clippy (deny warnings) + rustfmt check
	cargo clippy --target $(TARGET) --all-targets --no-default-features --features "$(FEATURES)" -- -D warnings
	cargo fmt --all -- --check

fmt: ## rustfmt
	cargo fmt --all

bench: build ## Raw generation throughput on the bundled fixture packs
	@for p in fixtures/packs/*/default/eventgen.conf; do echo "== $$p"; $(BIN) bench "$$p" --seconds 3; done

dist: ## Package the release binary as dist/firebox-<target>.tar.gz
	mkdir -p dist
	tar -C target/$(TARGET)/release -czf dist/firebox-$(TARGET).tar.gz firebox
	@ls -la dist

clean:
	cargo clean
	rm -rf dist
