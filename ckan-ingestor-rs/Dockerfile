# syntax=docker/dockerfile:1
# Rust builder — compiles the consumer binary

# --- Chef: compute a dependency recipe so deps can be cached as a real
# image layer (survives --cache-to type=registry), unlike --mount=type=cache
# which is ephemeral and is reset on every CI runner.
FROM rust:1.97.1-slim-trixie AS chef
RUN cargo install cargo-chef --locked --version 0.1.71
WORKDIR /src

# --- Builder: cook deps first (cached layer), then compile our own code ---
FROM chef AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    libssl-dev \
    libcurl4-openssl-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Prepare a recipe from the workspace manifests. `cargo metadata` (used by
# chef) requires each member to expose a target, so create placeholders that
# the real source will overwrite in the next stage.
COPY Cargo.toml Cargo.lock ./
COPY ckan-ingestor-lib/Cargo.toml ckan-ingestor-lib/
COPY ckan-ingestor-consumer/Cargo.toml ckan-ingestor-consumer/
RUN mkdir -p ckan-ingestor-lib/src ckan-ingestor-consumer/src && \
    touch ckan-ingestor-lib/src/lib.rs && \
    touch ckan-ingestor-consumer/src/main.rs && \
    cargo chef prepare --recipe-path recipe.json

# Cook downloads and compiles *all* dependencies into /src/target. This RUN
# layer is exported to the registry cache, so builds skip it unless deps change.
RUN cargo chef cook --release --recipe-path recipe.json

# Now copy the actual source and build only our crates.
# The production image only needs the application sources. Keeping tests,
# fixtures, and local metadata out of this COPY preserves the build cache.
COPY ckan-ingestor-lib/src/ ckan-ingestor-lib/src/
COPY ckan-ingestor-consumer/src/ ckan-ingestor-consumer/src/

# Build release binary. The shared /src/target can contain libduckdb.so for
# more than one platform, so resolve the exact Rust target triple and copy
# only this platform's lib — avoiding GNU cp's "will not overwrite just-created"
# failure (and copying the wrong arch) during multi-platform builds.
RUN DUCKDB_DOWNLOAD_LIB=1 \
    cargo build --release -p ckan_ingestor_consumer && \
    cp target/release/ckan-ingestor-consumer /usr/local/bin/ && \
    RUST_TARGET="$(rustc -vV | sed -n 's/^host: //p')" && \
    cp -f "target/duckdb-download/${RUST_TARGET}/1.5.5/libduckdb.so" /usr/local/lib/

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
