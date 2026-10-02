# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM golang:1.26-bookworm AS go-builder
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src/ingestor_orchestrator
COPY ingestor_orchestrator/iggy-message-transport ./iggy-message-transport
COPY ingestor_orchestrator/ingestor-job-message-contracts ./ingestor-job-message-contracts
COPY ingestor_orchestrator/sicom-source-identity ./sicom-source-identity
COPY ingestor_orchestrator/source-resource-catalog ./source-resource-catalog
COPY ingestor_orchestrator/sicom-crawler/go.mod ingestor_orchestrator/sicom-crawler/go.sum ./sicom-crawler/
WORKDIR /src/ingestor_orchestrator/sicom-crawler
RUN go mod download
COPY ingestor_orchestrator/sicom-crawler ./
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/sicom-crawler ./cmd/sicom-crawler

# Build natively for each target platform: V8 snapshots are architecture-specific.
FROM debian:trixie-slim AS lightpanda-builder
ARG TARGETPLATFORM
ARG LIGHTPANDA_REPO=https://github.com/guitcastro/browser.git
ARG LIGHTPANDA_REF=7e890133b78fa2127096e7464a3ee6977cc93d36
ARG ZIG_MINISIG=RWSGOq2NVecA2UPNdBUZykf1CCb147pkmdtYxgb3Ti+JO/wCYvhbAb/U
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl git xz-utils clang make pkg-config libglib2.0-dev minisign \
    && rm -rf /var/lib/apt/lists/*
RUN curl --fail -sSL --retry 3 -o /tmp/rustup.sh https://sh.rustup.rs \
    && sh /tmp/rustup.sh --profile minimal -y
ENV PATH="/root/.cargo/bin:${PATH}"
WORKDIR /browser
RUN git init && git remote add origin "$LIGHTPANDA_REPO" \
    && git fetch --depth 1 origin "$LIGHTPANDA_REF" && git checkout --detach FETCH_HEAD
RUN ZIG_VERSION=$(sed -n 's/.*minimum_zig_version = "\([^"]*\)".*/\1/p' build.zig.zon) \
    && case "$TARGETPLATFORM" in \
         linux/arm64) ZIG_ARCH=aarch64 ;; \
         linux/amd64) ZIG_ARCH=x86_64 ;; \
         *) exit 1 ;; \
       esac \
    && curl --fail -L --retry 3 -o /tmp/zig.tar.xz "https://ziglang.org/download/${ZIG_VERSION}/zig-${ZIG_ARCH}-linux-${ZIG_VERSION}.tar.xz" \
    && curl --fail -L --retry 3 -o /tmp/zig.tar.xz.minisig "https://ziglang.org/download/${ZIG_VERSION}/zig-${ZIG_ARCH}-linux-${ZIG_VERSION}.tar.xz.minisig" \
    && minisign -Vm /tmp/zig.tar.xz -P "$ZIG_MINISIG" \
    && mkdir /usr/local/zig && tar xf /tmp/zig.tar.xz -C /usr/local/zig --strip-components=1 \
    && ln -s /usr/local/zig/zig /usr/local/bin/zig
RUN make download-v8 && make build ZIGFLAGS=-Ddev_fast=false

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates tini \
    && rm -rf /var/lib/apt/lists/*
COPY --from=go-builder /out/sicom-crawler /usr/local/bin/sicom-crawler
COPY --from=lightpanda-builder /browser/zig-out/bin/lightpanda /usr/local/bin/lightpanda
ENV LIGHTPANDA_DISABLE_TELEMETRY=true LIGHTPANDA_DISABLE_CORE_DUMP=true XDG_CACHE_HOME=/tmp XDG_DATA_HOME=/tmp
USER 10001
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/sicom-crawler"]
