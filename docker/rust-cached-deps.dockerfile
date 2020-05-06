# Base image extends rust:nightly which extends debian:buster-slim
FROM ballistacompute/rust-base as build

# Fetch Ballista dependencies
WORKDIR /tmp
RUN USER=root cargo new ballista --lib
WORKDIR /tmp/ballista
COPY rust/Cargo.toml rust/Cargo.lock /tmp/ballista/
RUN cargo fetch

# Compile Ballista dependencies
RUN mkdir -p /tmp/ballista/src/bin/ && echo 'fn main() {}' >> /tmp/ballista/src/bin/executor.rs
RUN mkdir -p /tmp/ballista/proto
COPY proto/ballista.proto /tmp/ballista/proto/
COPY rust/build.rs /tmp/ballista/

# workaround for Arrow 0.17.0 build issue
RUN mkdir /format
COPY rust/format/Flight.proto /format

RUN cargo build --release --target x86_64-unknown-linux-musl

