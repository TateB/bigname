# syntax=docker/dockerfile:1.7

ARG RUST_VERSION=1.93.1

FROM rust:${RUST_VERSION}-bookworm AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends clang libclang-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY .cargo .cargo
COPY apps apps
COPY crates crates
COPY migrations migrations
COPY manifests manifests

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo build --locked --release --workspace --bins --features bigname-indexer/reth-db

FROM ubuntu:24.04 AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl libgcc-s1 libstdc++6 tini \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 bigname \
    && useradd --system --uid 10001 --gid bigname --home-dir /app --create-home bigname

WORKDIR /app

COPY --from=builder /app/target/release/bigname-api /usr/local/bin/bigname-api
COPY --from=builder /app/target/release/bigname-indexer /usr/local/bin/bigname-indexer
COPY --from=builder /app/target/release/bigname-worker /usr/local/bin/bigname-worker
COPY --from=builder --chown=bigname:bigname /app/manifests /app/manifests
COPY --chmod=0755 docker/entrypoint.sh /usr/local/bin/bigname

ENV BIGNAME_API_BIND_ADDR=0.0.0.0:3000 \
    BIGNAME_INDEXER_MANIFESTS_ROOT=/app/manifests/mainnet \
    RUST_LOG=info

EXPOSE 3000

USER bigname

ENTRYPOINT ["tini", "--", "bigname"]
CMD ["api"]
