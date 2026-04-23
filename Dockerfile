# syntax=docker/dockerfile:1.22

FROM rust:1.90-trixie AS builder
LABEL org.opencontainers.image.source="https://github.com/n-cloud-labs/gha-cache-server"
WORKDIR /usr/src/gha-cache-server

RUN apt-get update \
    && apt-get install --no-install-recommends -y build-essential cmake pkg-config protobuf-compiler sccache \
    && rm -rf /var/lib/apt/lists/*

# Create a dummy project to leverage Docker layer caching for dependencies
COPY Cargo.toml Cargo.lock ./
RUN mkdir src \
    && echo "fn main() {}" > src/main.rs \
    && cargo build --release --locked

# Copy the full source tree and build the release binary
COPY . .
RUN cargo build --release --locked --bin gha-cache-server

RUN mkdir -p /out/usr/local/bin /out/srv/gha-cache-server \
    && cp target/release/gha-cache-server /out/usr/local/bin/gha-cache-server \
    && cp -r migrations /out/srv/gha-cache-server/

FROM bitnami/minideb:trixie AS runtime
LABEL org.opencontainers.image.source="https://github.com/n-cloud-labs/gha-cache-server"
WORKDIR /srv/gha-cache-server

RUN install_packages ca-certificates

# Default configuration values. Override them with environment variables at runtime.
ENV PORT=8080 \
    RUST_LOG="info,github_actions_cache_rs=info" \
    MIMALLOC_ARENA_RESERVE=262144 \
    MAX_CONCURRENCY=64 \
    REQUEST_TIMEOUT_SECS=3600 \
    ENABLE_DIRECT_DOWNLOADS=true \
    BLOB_STORE=fs \
    FS_ROOT=/var/lib/gha-cache-server \
    DATABASE_URL=postgres://postgres:postgres@postgres:5432/cache

COPY --from=builder /out/ /

VOLUME ["/var/lib/gha-cache-server"]
EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/gha-cache-server"]
