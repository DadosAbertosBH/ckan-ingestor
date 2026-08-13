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

# Build dependencies using dummy sources — this layer is cached and reused
# as long as Cargo.toml/Cargo.lock don't change.
RUN mkdir -p ckan-ingestor-lib/src ckan-ingestor-consumer/src && \
    echo 'pub fn dummy() {}' > ckan-ingestor-lib/src/lib.rs && \
    echo 'fn main() {}' > ckan-ingestor-consumer/src/main.rs && \
    DUCKDB_DOWNLOAD_LIB=1 cargo build --release -p ckan_ingestor_consumer

# Copy actual source code
COPY ckan-ingestor-lib/ ckan-ingestor-lib/
COPY ckan-ingestor-consumer/ ckan-ingestor-consumer/

# Build release binary (dependencies already compiled in the previous layer)
RUN DUCKDB_DOWNLOAD_LIB=1 cargo build --release -p ckan_ingestor_consumer && \
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
