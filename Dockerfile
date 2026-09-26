# syntax=docker/dockerfile:1
# Multi-arch build from source. The builder runs natively on the build host
# and cross-compiles a static musl binary for the requested platform, so an
# amd64 runner produces the arm64 image without emulation.
#
#   docker buildx build --platform linux/amd64,linux/arm64 -t firebox .
#
# The same stage is what Stoker's worker image uses to embed the binary.

FROM --platform=$BUILDPLATFORM rust:1-bookworm AS builder
ARG TARGETPLATFORM
ARG FEATURES=tls
RUN apt-get update && apt-get install -y --no-install-recommends \
        musl-tools gcc-aarch64-linux-gnu \
    && rm -rf /var/lib/apt/lists/*
RUN case "$TARGETPLATFORM" in \
      linux/arm64) echo aarch64-unknown-linux-musl > /rust-target ;; \
      *)           echo x86_64-unknown-linux-musl  > /rust-target ;; \
    esac \
    && rustup target add "$(cat /rust-target)"
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY .cargo ./.cargo
COPY src ./src
# ring (behind the `tls` feature) compiles C for the target with these.
ENV CC_x86_64_unknown_linux_musl=musl-gcc \
    CC_aarch64_unknown_linux_musl=aarch64-linux-gnu-gcc \
    AR_aarch64_unknown_linux_musl=aarch64-linux-gnu-ar
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target,id=firebox-target-$TARGETPLATFORM \
    cargo build --release --target "$(cat /rust-target)" --features "$FEATURES" \
    && mkdir -p /out && cp "target/$(cat /rust-target)/release/firebox" /out/firebox

FROM scratch
COPY --from=builder /out/firebox /firebox
ENTRYPOINT ["/firebox"]
