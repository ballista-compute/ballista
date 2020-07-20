# Base image extends rust:nightly which extends debian:buster-slim
FROM ballistacompute/rust-base:0.3.0-alpha-0 as build

# Fetch Ballista dependencies
WORKDIR /tmp
RUN USER=root cargo new ballista --lib
WORKDIR /tmp/ballista
COPY rust/ballista/Cargo.toml /tmp/ballista/
RUN cargo fetch

# Compile Ballista dependencies
RUN mkdir -p /tmp/ballista/src/bin/ && echo 'fn main() {}' >> /tmp/ballista/src/bin/executor.rs
RUN mkdir -p /tmp/ballista/proto
COPY proto/ballista.proto /tmp/ballista/proto/
COPY rust/ballista/build.rs /tmp/ballista/
COPY rust/rust-toolchain /tmp/ballista/

RUN cargo build --release

