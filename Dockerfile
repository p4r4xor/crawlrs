# Multi-stage build using cargo-chef so the BoringSSL / wreq dependency
# graph is cached as its own image layer. First build is slow (boring-sys2
# compiles BoringSSL from source); subsequent builds with unchanged deps
# reuse the cooked layer and finish in seconds.
#
# Three stages:
#   chef    -> rust toolchain + cargo-chef installed
#   planner -> compute recipe.json from Cargo.lock
#   builder -> cook deps from recipe.json, then build the workspace
#   runtime -> debian:bookworm-slim + ca-certificates + the binary
#              (no rust toolchain, no build tools, non-root)

# syntax=docker/dockerfile:1.7

FROM rust:1.94.1-slim-bookworm AS chef
# build-essential pulls make + gcc + libc6-dev so CMake-driven builds
# (boring-sys2 / BoringSSL) can find a working build tool.
RUN apt-get update && apt-get install --no-install-recommends -y \
        build-essential \
        pkg-config \
        libssl-dev \
        cmake \
        clang \
        ca-certificates \
        git \
    && rm -rf /var/lib/apt/lists/* \
    && cargo install cargo-chef --locked
WORKDIR /build

# ---- planner: produce a recipe.json that cargo-chef can cook ------------
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ---- builder: cook deps in a cacheable layer, then build the workspace --
FROM chef AS builder
COPY --from=planner /build/recipe.json recipe.json
# The cooked layer is the load-bearing cache. Re-runs of `docker build`
# with unchanged Cargo.lock + Cargo.toml hit this layer and skip ~10min
# of BoringSSL recompilation.
RUN cargo chef cook --release --recipe-path recipe.json -p crawlrs-bin
COPY . .
RUN cargo build --release -p crawlrs-bin
# Note: we intentionally do NOT strip. Release-profile `debug = 1`
# (in workspace Cargo.toml) keeps line tables so heap profiles
# from jemalloc resolve to function names + source lines via
# `jeprof`. Adds ~30-50 MB to the image but is non-negotiable for
# evidence-driven performance work.

# ---- runtime: minimal Debian + ca-certs + the binary; non-root ----------
FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install --no-install-recommends -y \
        ca-certificates \
        tini \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -g 1000 crawlrs \
    && useradd -u 1000 -g 1000 -m -s /bin/bash crawlrs

COPY --from=builder --chown=root:root /build/target/release/crawlrs /usr/local/bin/crawlrs
COPY --chown=root:root docker/entrypoints/prod/crawl.sh /usr/local/bin/crawl.sh
RUN chmod 0755 /usr/local/bin/crawl.sh

USER crawlrs:1000
WORKDIR /home/crawlrs

EXPOSE 9090

# tini reaps zombies and forwards SIGTERM cleanly to the binary so the
# graceful-shutdown path (mark /readyz unhealthy -> drain workers ->
# flush stores -> exit) runs end-to-end.
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/crawl.sh"]
CMD ["crawl", "--config", "/etc/crawlrs/crawl.toml"]
