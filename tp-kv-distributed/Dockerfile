# =============================================================================
# Dockerfile multi-stage pour kv-distributed
# =============================================================================
# Stage 1 : build avec toolchain Rust complète
# Stage 2 : image runtime minimale (distroless)
# =============================================================================

# ----------- Stage 1 : build -----------
FROM rust:1.82-slim AS builder

WORKDIR /build

# Dépendances système minimales
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Cache des dépendances : on copie d'abord Cargo.* puis on stub les sources
COPY Cargo.toml Cargo.lock* ./
RUN mkdir -p src benches \
    && echo "fn main() {}" > src/main.rs \
    && echo "" > src/lib.rs \
    && mkdir -p src/bin \
    && echo "fn main() {}" > src/bin/kvc.rs \
    && echo "fn main() {}" > benches/storage_bench.rs \
    && cargo build --release --bins \
    && rm -rf src benches target/release/deps/kv_distributed* \
              target/release/kv-distributed target/release/kvc

# Copie du vrai code
COPY src ./src
COPY benches ./benches
COPY proto ./proto

# Build release
RUN cargo build --release --bins

# ----------- Stage 2 : runtime -----------
FROM gcr.io/distroless/cc-debian12

LABEL org.opencontainers.image.title="kv-distributed"
LABEL org.opencontainers.image.description="TP Jour 4 Plan 3 – KV store distribué"
LABEL org.opencontainers.image.source="formation-rust-ovh"

WORKDIR /app

COPY --from=builder /build/target/release/kv-distributed /app/kv-distributed
COPY --from=builder /build/target/release/kvc /app/kvc

EXPOSE 50051 9090

ENTRYPOINT ["/app/kv-distributed"]
