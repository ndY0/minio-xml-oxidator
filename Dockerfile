FROM rust:1.87 AS build

RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*

WORKDIR /app
RUN cargo init --name minio-xml-validator .

COPY Cargo.toml Cargo.lock ./
RUN mkdir -p proto && echo 'syntax = "proto3";' > proto/validator.proto
COPY build.rs .
RUN cargo build --release 2>/dev/null || true

RUN rm -rf src proto
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=build /app/target/release/minio-xml-validator /usr/local/bin/minio-xml-validator
COPY --from=build /app/config.toml /etc/minio-xml-validator/config.toml

ENTRYPOINT ["minio-xml-validator", "/etc/minio-xml-validator/config.toml"]
