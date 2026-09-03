# syntax=docker/dockerfile:1
# Rust builder — compiles the selected worker binary

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
    curl \
    libssl-dev \
    libcurl4-openssl-dev \
    pkg-config \
    unzip \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Prepare a recipe from the workspace manifests. `cargo metadata` (used by
# chef) requires each member to expose a target, so create placeholders that
# the real source will overwrite in the next stage.
COPY Cargo.toml Cargo.lock ./
COPY ckan-ingestor-lib/Cargo.toml ckan-ingestor-lib/
COPY ckan-metadata-ingestor/Cargo.toml ckan-metadata-ingestor/
COPY ckan-ingestor-consumer/Cargo.toml ckan-ingestor-consumer/
COPY ckan-worker-coordinator/Cargo.toml ckan-worker-coordinator/
RUN mkdir -p ckan-ingestor-lib/src ckan-metadata-ingestor/src ckan-ingestor-consumer/src ckan-worker-coordinator/src && \
    touch ckan-ingestor-lib/src/lib.rs && \
    touch ckan-metadata-ingestor/src/lib.rs && \
    touch ckan-ingestor-consumer/src/main.rs && \
    touch ckan-worker-coordinator/src/main.rs && \
    cargo chef prepare --recipe-path recipe.json

# Cook downloads and compiles *all* dependencies into /src/target. This RUN
# layer is exported to the registry cache, so builds skip it unless deps change.
RUN cargo chef cook --release --recipe-path recipe.json

# Now copy the actual source and build only our crates.
# The production image only needs the application sources. Keeping tests,
# fixtures, and local metadata out of this COPY preserves the build cache.
COPY ckan-ingestor-lib/src/ ckan-ingestor-lib/src/
COPY ckan-metadata-ingestor/src/ ckan-metadata-ingestor/src/
COPY ckan-ingestor-consumer/src/ ckan-ingestor-consumer/src/
COPY ckan-worker-coordinator/src/ ckan-worker-coordinator/src/

# The Python API uses DuckDB 1.5.5, whose DuckLake extension supports catalog
# format 1.0. Download the same engine for the worker so both clients can open
# the shared PostgreSQL-backed DuckLake catalog. duckdb-rs 1.10501 supplies a
# compatible C ABI binding for this patch release.
ARG DUCKDB_VERSION=1.5.5
ARG WORKER_PACKAGE=ckan_ingestor_consumer
ARG WORKER_BINARY=ckan-ingestor-consumer
RUN ARCH="$(dpkg --print-architecture)" && \
    case "$ARCH" in \
        amd64) DUCKDB_ARCH=amd64 ;; \
        arm64) DUCKDB_ARCH=arm64 ;; \
        *) echo "Unsupported DuckDB architecture: $ARCH" >&2; exit 1 ;; \
    esac && \
    mkdir -p /opt/duckdb && \
    curl --fail --location --silent --show-error \
      "https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/libduckdb-linux-${DUCKDB_ARCH}.zip" \
      --output /tmp/libduckdb.zip && \
    unzip -q /tmp/libduckdb.zip -d /opt/duckdb && \
    DUCKDB_LIB_DIR=/opt/duckdb DUCKDB_INCLUDE_DIR=/opt/duckdb \
    cargo build --release -p ${WORKER_PACKAGE} && \
    cp target/release/${WORKER_BINARY} /usr/local/bin/worker && \
    cp -f /opt/duckdb/libduckdb.so /usr/local/lib/

# --- Runtime image ---
FROM debian:trixie-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/bin/worker /usr/local/bin/
COPY --from=builder /usr/local/lib/libduckdb.so /usr/local/lib/
RUN ldconfig

ENV RUST_LOG=info

ENTRYPOINT ["worker"]
