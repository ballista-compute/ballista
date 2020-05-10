# Base image extends rust:nightly which extends debian:buster-slim
FROM ballistacompute/rust-base:0.2.5 as build

# Fetch Ballista dependencies
WORKDIR /tmp
RUN USER=root cargo new ballista --lib
WORKDIR /tmp/ballista
COPY rust/ballista/Cargo.toml rust/ballista/Cargo.lock /tmp/ballista/
RUN cargo fetch

# Compile Ballista dependencies
RUN mkdir -p /tmp/ballista/src/bin/ && echo 'fn main() {}' >> /tmp/ballista/src/bin/executor.rs
RUN mkdir -p /tmp/ballista/proto
COPY proto/ballista.proto /tmp/ballista/proto/
COPY rust/ballista/build.rs /tmp/ballista/

# workaround for Arrow 0.17.0 build issue
RUN mkdir /format
COPY rust/ballista/format/Flight.proto /format

RUN cargo build --release

