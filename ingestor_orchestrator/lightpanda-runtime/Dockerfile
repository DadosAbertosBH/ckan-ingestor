# syntax=docker/dockerfile:1
# Native Lightpanda builds are architecture-specific because of their V8 snapshot.
FROM debian:trixie-slim AS lightpanda-builder
ARG TARGETPLATFORM
ARG LIGHTPANDA_REPO=https://github.com/guitcastro/browser.git
ARG LIGHTPANDA_REF=7e890133b78fa2127096e7464a3ee6977cc93d36
ARG ZIG_MINISIG=RWSGOq2NVecA2UPNdBUZykf1CCb147pkmdtYxgb3Ti+JO/wCYvhbAb/U
# Bootstrap Debian's trusted roots before switching its package source to HTTPS.
# The local network policy redirects large HTTP downloads, while HTTPS succeeds
# once the certificate bundle is present.
RUN apt-get -o Acquire::Retries=5 -o Acquire::http::Pipeline-Depth=0 update && apt-get -o Acquire::Retries=5 -o Acquire::http::Pipeline-Depth=0 install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
RUN sed -i 's|http://deb.debian.org|https://deb.debian.org|g' /etc/apt/sources.list.d/debian.sources && apt-get -o Acquire::Retries=5 update && apt-get -o Acquire::Retries=5 install -y --no-install-recommends curl git xz-utils clang make pkg-config libglib2.0-dev minisign && rm -rf /var/lib/apt/lists/*
RUN curl --fail -sSL --retry 3 -o /tmp/rustup.sh https://sh.rustup.rs && sh /tmp/rustup.sh --profile minimal -y
ENV PATH="/root/.cargo/bin:${PATH}"
WORKDIR /browser
RUN git init && git remote add origin "$LIGHTPANDA_REPO" && git fetch --depth 1 origin "$LIGHTPANDA_REF" && git checkout --detach FETCH_HEAD
RUN ZIG_VERSION=$(sed -n 's/.*minimum_zig_version = "\([^"]*\)".*/\1/p' build.zig.zon) && case "$TARGETPLATFORM" in linux/arm64) ZIG_ARCH=aarch64 ;; linux/amd64) ZIG_ARCH=x86_64 ;; *) exit 1 ;; esac && curl --fail -L --retry 3 -o /tmp/zig.tar.xz "https://ziglang.org/download/${ZIG_VERSION}/zig-${ZIG_ARCH}-linux-${ZIG_VERSION}.tar.xz" && curl --fail -L --retry 3 -o /tmp/zig.tar.xz.minisig "https://ziglang.org/download/${ZIG_VERSION}/zig-${ZIG_ARCH}-linux-${ZIG_VERSION}.tar.xz.minisig" && minisign -Vm /tmp/zig.tar.xz -P "$ZIG_MINISIG" && mkdir /usr/local/zig && tar xf /tmp/zig.tar.xz -C /usr/local/zig --strip-components=1 && ln -s /usr/local/zig/zig /usr/local/bin/zig
RUN mkdir -p /root/.cache/zig/tmp
# Downloads Lightpanda's prebuilt V8 archive; this avoids a V8 source build.
RUN make download-v8
RUN make build ZIGFLAGS=-Ddev_fast=false

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates tini && rm -rf /var/lib/apt/lists/*
COPY --from=lightpanda-builder /browser/zig-out/bin/lightpanda /usr/local/bin/lightpanda
ENV LIGHTPANDA_DISABLE_TELEMETRY=true LIGHTPANDA_DISABLE_CORE_DUMP=true XDG_CACHE_HOME=/tmp XDG_DATA_HOME=/tmp
USER 10001
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/lightpanda"]
