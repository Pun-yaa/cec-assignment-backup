FROM rust:1.90.0-bookworm AS builder
RUN apt-get update && apt-get install -y --no-install-recommends cmake libssl-dev pkg-config && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY .sqlx ./.sqlx
COPY event-hash ./event-hash
COPY experiment-producer ./experiment-producer
COPY notifications-service ./notifications-service
COPY http-load-generator ./http-load-generator
COPY test-to-api ./test-to-api
COPY notifier ./notifier
ENV SQLX_OFFLINE=true
RUN cargo build --locked --release -j 1 --bin experiment-producer --bin notifications-service --bin http-load-generator

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/experiment-producer /app/target/release/notifications-service /app/target/release/http-load-generator /usr/local/bin/
COPY experiment-producer/schemas ./experiment-producer/schemas
RUN mkdir -p notifications-service && touch experiment-producer/.env notifications-service/.env
