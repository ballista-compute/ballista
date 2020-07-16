# Base image extends rust:nightly which extends debian:buster-slim
FROM ballistacompute/rust-cached-deps:0.3.0-SNAPSHOT as build

# protobuf
COPY proto/ballista.proto /tmp/ballista/proto/

# Compile Ballista
RUN rm -rf /tmp/ballista/src/

#TODO relly need to copy whole project in, not just ballista crate, so we pick up the correct Cargo.lock
RUN rm -rf /tmp/ballista/Cargo.lock

COPY rust/ballista/Cargo.toml /tmp/ballista/
COPY rust/ballista/build.rs /tmp/ballista/
COPY rust/ballista/src/ /tmp/ballista/src/

# Benchmark source
RUN mkdir -p /tmp/benchmarks/src
WORKDIR /tmp/benchmarks/
COPY rust/benchmarks/Cargo.* /tmp/benchmarks/
COPY rust/benchmarks/src/ /tmp/benchmarks/src/
COPY rust/rust-toolchain /tmp/benchmarks/
RUN cargo build --release

# Copy the binary into a new container for a smaller docker image
FROM debian:buster-slim

COPY --from=build /tmp/benchmarks/target/release/ballista-benchmarks /
USER root

ENV RUST_LOG=info
ENV RUST_BACKTRACE=full

CMD ["/ballista-benchmarks"]
