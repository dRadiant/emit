# Multi-stage build for emit-engine.
#
# Stage 1 (builder): Alpine + Zig 0.15.2. Cross-compiles to x86_64-linux-musl
# for a fully static binary. eth.zig's C sources (xkcp Keccak, secp256k1)
# compile cleanly against musl. rocksdb-zig is a lazy dep and is not pulled
# by `zig build`, so the import path is excluded from this image — operators
# wanting the RocksDB direct-import path build that separately on the host.
#
# Stage 2 (runtime): scratch + the static binary. No shell, no package
# manager, no libc. Image target: under 30 MB.
#
# Build:   docker build -t emit-engine .
# Run:     docker run --rm emit-engine --help
ARG ZIG_VERSION=0.15.2
ARG TARGET=x86_64-linux-musl

FROM alpine:3.20 AS builder
ARG ZIG_VERSION
ARG TARGET

RUN apk add --no-cache curl xz tar ca-certificates

# Pin Zig to a known good version. The 0.14+ tarball naming uses
# `zig-<arch>-<os>` (arch before os); older releases used the reverse.
RUN curl -fsSL "https://ziglang.org/download/${ZIG_VERSION}/zig-x86_64-linux-${ZIG_VERSION}.tar.xz" \
    | tar -xJ -C /opt \
    && ln -s "/opt/zig-x86_64-linux-${ZIG_VERSION}/zig" /usr/local/bin/zig

WORKDIR /src
COPY build.zig build.zig.zon ./
COPY core ./core
COPY engine ./engine
COPY sdk ./sdk

# ReleaseFast + musl static. Output: /src/zig-out/bin/emit-engine
RUN zig build -Doptimize=ReleaseFast -Dtarget=${TARGET}

FROM scratch AS runtime
COPY --from=builder /src/zig-out/bin/emit-engine /emit-engine

ENTRYPOINT ["/emit-engine"]
