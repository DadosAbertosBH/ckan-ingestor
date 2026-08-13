# syntax=docker/dockerfile:1
# Rust builder — compiles the consumer binary
FROM rust:1.97.1-slim-trixie AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    libssl-dev \
    libcurl4-openssl-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Copy workspace manifests first (layer caching for dependency resolution)
COPY Cargo.toml Cargo.lock ./
COPY ckan-ingestor-lib/Cargo.toml ckan-ingestor-lib/
COPY ckan-ingestor-consumer/Cargo.toml ckan-ingestor-consumer/

# Copy actual source code
COPY ckan-ingestor-lib/ ckan-ingestor-lib/
COPY ckan-ingestor-consumer/ ckan-ingestor-consumer/

# Build release binary (cargo registry and target caches are mounted so
# they persist via the registry cache between builds).
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    DUCKDB_DOWNLOAD_LIB=1 \
    cargo build --release -p ckan_ingestor_consumer && \
    cp target/release/ckan-ingestor-consumer /usr/local/bin/ && \
    cp target/duckdb-download/*/1.5.5/libduckdb.so /usr/local/lib/

# --- Runtime image ---
FROM debian:trixie-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/bin/ckan-ingestor-consumer /usr/local/bin/
COPY --from=builder /usr/local/lib/libduckdb.so /usr/local/lib/
RUN ldconfig

ENV RUST_LOG=info

ENTRYPOINT ["ckan-ingestor-consumer"]
