# Build context is this repo (mgo_backend) itself — lms_react is pulled
# straight from GitHub during the build instead of being COPYd from a local
# sibling checkout, so this works from a plain Git-source OpenShift
# BuildConfig (which can only clone one repo) as well as a local build:
#   docker build -f Dockerfile -t mgo-backend:test .
# (run from inside mgo_backend/)

# ── Stage 1: Build frontend ──
FROM node:22-slim AS frontend-builder

RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /frontend

# Pin to a specific branch/tag here once lms_react has stable releases —
# right now this always builds whatever's currently on main.
RUN git clone --depth=1 https://github.com/aaely/lms_react.git .
RUN npm ci
RUN npm run build

# ── Stage 2: Build Rust binary ──
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

# ── Stage 3: Runtime ──
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y libssl3 ca-certificates && rm -rf /var/lib/apt/lists/*

# If your AD domain controller's cert (for LDAP_URL=ldaps://...) is signed by
# an internal/private CA rather than a public one, this container won't trust
# it — only the standard public CA bundle is installed above. Once you have
# the CA's .crt file from GM IT, drop it at certs/<name>.crt and uncomment
# the two lines below.
COPY certs/*.crt /usr/local/share/ca-certificates/
RUN update-ca-certificates

WORKDIR /app

COPY --from=builder /app/target/release/rocket_http .
COPY docker-entrypoint.sh .
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
