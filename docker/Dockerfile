# syntax=docker/dockerfile:1.7
FROM rust:1.92-slim AS builder

ENV CARGO_HOME=/usr/local/cargo
ARG RUSTFLAGS="-C target-cpu=native"
ENV RUSTFLAGS=${RUSTFLAGS}

WORKDIR /build/rust_api

ENV CARGO_TARGET_DIR=/build/target-cache

COPY rust_api/Cargo.toml rust_api/Cargo.lock ./
COPY rust_api/src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/build/target-cache \
    cargo build --release --bin rust_api --bin trading_init_db --locked && \
    mkdir -p /build/out && \
    cp /build/target-cache/release/rust_api /build/out/rust_api && \
    cp /build/target-cache/release/trading_init_db /build/out/trading_init_db

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /build/out/rust_api /usr/local/bin/trading_api
COPY --from=builder /build/out/trading_init_db /usr/local/bin/trading_init_db
COPY schema.sql /app/schema.sql

EXPOSE 8000

CMD ["trading_api"]
