# ── Stage 1: Build Rust binary ──
FROM rust:1.95-slim-bookworm AS builder

RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache dependencies before copying source
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs
RUN cargo build --release
RUN rm -f target/release/deps/rocket_http*

COPY src ./src
RUN cargo build --release

# ── Stage 2: Runtime ──
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y libssl3 ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/rocket_http .
COPY docker-entrypoint.sh .
RUN chmod +x docker-entrypoint.sh
COPY dist ./dist

# OpenShift runs containers with an arbitrary UID in group 0.
# chown 1001:0 + chmod g=u lets any UID in group 0 read/exec these files.
RUN chown -R 1001:0 /app && chmod -R g=u /app

USER 1001

# HTTP / WS (plain)
EXPOSE 8000 9001
# HTTPS / WSS (TLS — only used when USE_HTTPS=true / --https flag)
EXPOSE 8443

ENTRYPOINT ["./docker-entrypoint.sh"]
