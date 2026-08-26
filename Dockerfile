# Build context for this Dockerfile must be the PARENT directory containing
# both `mgo_backend` and `lms_react` as sibling checkouts, e.g.:
#   docker build -f mgo_backend/Dockerfile -t mgo-backend:test .
# (run from the directory that contains both repos, not from mgo_backend/ itself)

# ── Stage 1: Build frontend ──
FROM node:22-slim AS frontend-builder

WORKDIR /frontend

# Cache npm install before copying full source
COPY lms_react/package.json lms_react/package-lock.json ./
RUN npm ci

COPY lms_react ./
RUN npm run build

# ── Stage 2: Build Rust binary ──
FROM rust:1.95-slim-bookworm AS builder

RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache dependencies before copying source
COPY mgo_backend/Cargo.toml mgo_backend/Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs
RUN cargo build --release
RUN rm -f target/release/deps/rocket_http*

COPY mgo_backend/src ./src
RUN cargo build --release

# ── Stage 3: Runtime ──
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y libssl3 ca-certificates && rm -rf /var/lib/apt/lists/*

# If your AD domain controller's cert (for LDAP_URL=ldaps://...) is signed by
# an internal/private CA rather than a public one, this container won't trust
# it — only the standard public CA bundle is installed above. Once you have
# the CA's .crt file from GM IT, drop it at mgo_backend/certs/<name>.crt and
# uncomment the two lines below.
# COPY mgo_backend/certs/*.crt /usr/local/share/ca-certificates/
# RUN update-ca-certificates

WORKDIR /app

COPY --from=builder /app/target/release/rocket_http .
COPY mgo_backend/docker-entrypoint.sh .
RUN chmod +x docker-entrypoint.sh
COPY --from=frontend-builder /frontend/dist ./dist

# OpenShift runs containers with an arbitrary UID in group 0.
# chown 1001:0 + chmod g=u lets any UID in group 0 read/exec these files.
RUN chown -R 1001:0 /app && chmod -R g=u /app

USER 1001

# HTTP / WS (plain)
EXPOSE 8000 9001
# HTTPS / WSS (TLS — only used when USE_HTTPS=true / --https flag)
EXPOSE 8443

ENTRYPOINT ["./docker-entrypoint.sh"]
