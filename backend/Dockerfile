# Minimal Alpine base image pinned to Rust 1.98.1
FROM rust:1.98.1-alpine

# Install build dependencies, musl tools, openssl, and cargo-watch
RUN apk add --no-cache \
    musl-dev \
    gcc \
    libc-dev \
    pkgconfig \
    openssl-dev \
    cargo-watch \
    curl

# Pre-install rustfmt and clippy
RUN rustup component add rustfmt clippy

WORKDIR /app

# Ensure cargo target directory is segregated to avoid host file sync issues
ENV CARGO_TARGET_DIR=/app/target

EXPOSE 8000

# Default command runs with cargo-watch for hot-reloading during development
CMD ["cargo", "watch", "-x", "run"]
