# Strata Distributed File System
# Multi-stage build for minimal production image

# =============================================================================
# Stage 1: Build environment
# =============================================================================
FROM rust:1.75-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    libfuse3-dev \
    libclang-dev \
    clang \
    pkg-config \
    librocksdb-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy manifests first for dependency caching
COPY Cargo.toml Cargo.lock* ./

# Create a dummy main.rs to build dependencies
RUN mkdir -p src/bin && \
    echo "fn main() {}" > src/main.rs && \
    echo "fn main() {}" > src/bin/server.rs && \
    echo "fn main() {}" > src/bin/fuse_mount.rs && \
    echo "pub fn lib() {}" > src/lib.rs

# Build dependencies (this layer will be cached)
RUN cargo build --release 2>/dev/null || true

# Remove dummy source files
RUN rm -rf src

# Copy actual source code
COPY src ./src
COPY benches ./benches
COPY tests ./tests
COPY build.rs* ./

# Build the actual application
RUN cargo build --release --features "s3" && \
    strip /build/target/release/strata && \
    strip /build/target/release/strata-server

# =============================================================================
# Stage 2: Runtime environment (minimal)
# =============================================================================
FROM debian:bookworm-slim AS runtime

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libfuse3-3 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r strata && useradd -r -g strata strata

# Create necessary directories
RUN mkdir -p /var/lib/strata/data \
    /var/lib/strata/metadata \
    /var/lib/strata/logs \
    /etc/strata \
    && chown -R strata:strata /var/lib/strata

# Copy binaries from builder
COPY --from=builder /build/target/release/strata /usr/local/bin/
COPY --from=builder /build/target/release/strata-server /usr/local/bin/

# Copy default configuration
COPY --chmod=644 docker/config.toml /etc/strata/config.toml

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD /usr/local/bin/strata health || exit 1

# Expose ports
# 8080 - S3 Gateway
# 9090 - Metadata gRPC
# 9091 - Data gRPC
# 9100 - Prometheus metrics
EXPOSE 8080 9090 9091 9100

# Set environment variables
ENV STRATA_CONFIG=/etc/strata/config.toml \
    STRATA_DATA_DIR=/var/lib/strata/data \
    STRATA_METADATA_DIR=/var/lib/strata/metadata \
    STRATA_LOG_DIR=/var/lib/strata/logs \
    RUST_LOG=info

# Run as non-root user
USER strata

# Default entrypoint
ENTRYPOINT ["/usr/local/bin/strata-server"]
CMD ["--config", "/etc/strata/config.toml"]

# =============================================================================
# Stage 3: FUSE-enabled image (for mounting)
# =============================================================================
FROM runtime AS fuse

USER root

# Install FUSE utilities
RUN apt-get update && apt-get install -y \
    fuse3 \
    && rm -rf /var/lib/apt/lists/*

# Copy FUSE binary
COPY --from=builder /build/target/release/strata-fuse /usr/local/bin/

# FUSE requires privileged mode, set appropriate capabilities
# Note: Container must be run with --privileged or --cap-add SYS_ADMIN --device /dev/fuse

USER strata

ENTRYPOINT ["/usr/local/bin/strata-fuse"]
CMD ["--help"]

# =============================================================================
# Stage 4: Development image (includes tools)
# =============================================================================
FROM rust:1.75-bookworm AS development

RUN apt-get update && apt-get install -y \
    libfuse3-dev \
    libclang-dev \
    clang \
    pkg-config \
    librocksdb-dev \
    fuse3 \
    curl \
    vim \
    htop \
    && rm -rf /var/lib/apt/lists/*

# Install useful Rust tools
RUN cargo install cargo-watch cargo-audit cargo-outdated

WORKDIR /workspace

# Keep container running for development
CMD ["bash"]
