# Build context is this repo (mgo_backend) itself.
# lms_react is pulled directly from GitHub during the build.
#
# Local build with a sibling lms_react checkout:
#   $envB64 = [Convert]::ToBase64String(
#     [IO.File]::ReadAllBytes("..\lms_react\.env.production")
#   )
#   docker build --build-arg "FRONTEND_ENV_B64=$envB64" `
#     -f Dockerfile -t mgo-backend:test .
#
# OpenShift BuildConfig must provide FRONTEND_ENV_B64 unless
# .env.production is committed in the lms_react repository.

# ── Stage 1: Build frontend ──
FROM node:22-slim AS frontend-builder

RUN apt-get update && \
    apt-get install -y --no-install-recommends git ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /frontend

# Currently follows the repository's default branch.
# Pin this to a stable tag or commit when available.
RUN git clone --depth=1 https://github.com/aaely/lms_react.git .

# Optional base64-encoded frontend environment file.
# This avoids COPYing a file outside the mgo_backend build context.
ARG FRONTEND_ENV_B64=""

RUN if [ -n "$FRONTEND_ENV_B64" ]; then \
        printf '%s' "$FRONTEND_ENV_B64" | base64 -d > .env.production; \
    elif [ -f .env.production ]; then \
        echo "Using .env.production from the cloned lms_react repository"; \
    else \
        echo "ERROR: .env.production was not provided." >&2; \
        echo "Provide FRONTEND_ENV_B64 or commit .env.production to lms_react." >&2; \
        exit 1; \
    fi

RUN npm ci
RUN npm run build

# ── Stage 2: Build Rust binary ──
FROM rust:1.95-slim-bookworm AS builder

RUN apt-get update && \
    apt-get install -y --no-install-recommends pkg-config libssl-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache dependencies before copying source
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs
RUN cargo build --release
RUN rm -f target/release/deps/modms*

COPY src ./src
RUN cargo build --release

# ── Stage 3: Runtime ──
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends libssl3 ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Runtime CA certificates are mounted from an OpenShift Secret.
RUN mkdir -p /usr/local/share/ca-certificates/custom && \
    chown -R 1001:0 /usr/local/share/ca-certificates /etc/ssl/certs && \
    chmod -R g=u /usr/local/share/ca-certificates /etc/ssl/certs

WORKDIR /app

COPY --from=builder /app/target/release/rocket_http .
COPY docker-entrypoint.sh .
RUN chmod +x docker-entrypoint.sh

COPY --from=frontend-builder /frontend/dist ./dist

# OpenShift arbitrary UID support
RUN chown -R 1001:0 /app && \
    chmod -R g=u /app

USER 1001

# HTTP / WS
EXPOSE 8000 9001

# HTTPS / WSS
EXPOSE 8443

ENTRYPOINT ["./docker-entrypoint.sh"]
