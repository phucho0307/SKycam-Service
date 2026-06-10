# syntax=docker/dockerfile:1.7
ARG RUST_VERSION=1.96
ARG APP_BIN=api

FROM rust:${RUST_VERSION}-bookworm AS chef
RUN cargo install cargo-chef --locked
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
ARG APP_BIN
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release --bin ${APP_BIN}
RUN cp target/release/${APP_BIN} /app/server

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/server /usr/local/bin/server
ENV ROCKET_ADDRESS=0.0.0.0
EXPOSE 8000
USER 1000:1000
ENTRYPOINT ["/usr/local/bin/server"]
