# syntax=docker/dockerfile:1
# Rust builder — compiles the selected worker binary

# --- Chef: compute a dependency recipe so deps can be cached as a real
# image layer (survives --cache-to type=registry), unlike --mount=type=cache
# which is ephemeral and is reset on every CI runner.
FROM rustlang/rust:nightly-slim AS chef
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
COPY ckan-ingestor-worker-lib/Cargo.toml ckan-ingestor-worker-lib/
COPY message-processor/Cargo.toml message-processor/
COPY iggy-processor/Cargo.toml iggy-processor/
COPY ckan-metadata-ingestor/Cargo.toml ckan-metadata-ingestor/
COPY ckan-ingestor-consumer/Cargo.toml ckan-ingestor-consumer/
COPY ckan-worker-coordinator/Cargo.toml ckan-worker-coordinator/
RUN mkdir -p ckan-ingestor-lib/src ckan-ingestor-worker-lib/src message-processor/src iggy-processor/src ckan-metadata-ingestor/src ckan-ingestor-consumer/src ckan-worker-coordinator/src && \
    for target in \
        ckan-ingestor-lib/src/lib.rs \
        ckan-ingestor-worker-lib/src/lib.rs \
        message-processor/src/lib.rs \
        iggy-processor/src/lib.rs \
        ckan-metadata-ingestor/src/lib.rs \
        ckan-ingestor-consumer/src/main.rs \
        ckan-worker-coordinator/src/main.rs; do \
        printf '#![allow(unused_crate_dependencies)]\\n' > "$target"; \
    done && \
    cargo chef prepare --recipe-path recipe.json

# Cook downloads and compiles *all* dependencies into /src/target. This RUN
# layer is exported to the registry cache, so builds skip it unless deps change.
RUN cargo chef cook --release --recipe-path recipe.json

# Now copy the actual source and build only our crates.
# The production image only needs the application sources. Keeping tests,
# fixtures, and local metadata out of this COPY preserves the build cache.
COPY ckan-ingestor-lib/src/ ckan-ingestor-lib/src/
COPY ckan-ingestor-worker-lib/src/ ckan-ingestor-worker-lib/src/
COPY message-processor/src/ message-processor/src/
COPY iggy-processor/src/ iggy-processor/src/
COPY ckan-metadata-ingestor/src/ ckan-metadata-ingestor/src/
COPY ckan-ingestor-consumer/src/ ckan-ingestor-consumer/src/
COPY ckan-worker-coordinator/src/ ckan-worker-coordinator/src/

ARG WORKER_PACKAGE=ckan_ingestor_consumer
ARG WORKER_BINARY=ckan-ingestor-consumer
RUN cargo build --release -p ${WORKER_PACKAGE} && \
    cp target/release/${WORKER_BINARY} /usr/local/bin/worker

# --- Runtime image ---
FROM debian:trixie-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/bin/worker /usr/local/bin/

ENV RUST_LOG=info \
    RAYON_NUM_THREADS=1

ENTRYPOINT ["worker"]
