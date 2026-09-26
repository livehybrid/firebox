# syntax=docker/dockerfile:1
# Multi-arch build from source. The builder runs natively on the build host
# and cross-compiles a static musl binary for the requested platform, so an
# amd64 runner produces the arm64 image without emulation.
#
#   docker buildx build --platform linux/amd64,linux/arm64 -t firebox .
#
# The same stage is what Stoker's worker image uses to embed the binary.

# The default feature set is pure Rust (plain-HTTP HEC), so no C cross
# toolchain is needed: the musl targets link with rustc's bundled rust-lld.
# `--build-arg FEATURES=tls` adds HTTPS via ring and then needs a C compiler
# with musl headers for the target (CI uses zig cc for arm64).
FROM --platform=$BUILDPLATFORM rust:1-bookworm AS builder
ARG TARGETPLATFORM
ARG FEATURES=http
RUN case "$TARGETPLATFORM" in \
      linux/arm64) echo aarch64-unknown-linux-musl > /rust-target ;; \
      *)           echo x86_64-unknown-linux-musl  > /rust-target ;; \
    esac \
    && rustup target add "$(cat /rust-target)"
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY .cargo ./.cargo
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target,id=firebox-target-$TARGETPLATFORM \
    cargo build --release --locked --target "$(cat /rust-target)" --no-default-features --features "$FEATURES" \
    && mkdir -p /out && cp "target/$(cat /rust-target)/release/firebox" /out/firebox

FROM scratch
COPY --from=builder /out/firebox /firebox
ENTRYPOINT ["/firebox"]
