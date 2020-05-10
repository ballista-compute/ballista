# Base image extends rust:nightly which extends debian:buster-slim
FROM ballistacompute/rust-cached-deps:0.2.5 as build

# protobuf
COPY proto/ballista.proto /tmp/ballista/proto/

# Add Ballista
RUN rm -rf /tmp/ballista/src/
COPY rust/ballista/Cargo.* /tmp/ballista/
COPY rust/ballista/build.rs /tmp/ballista/
COPY rust/ballista/src/ /tmp/ballista/src/
# workaround for Arrow 0.17.0 build issue
COPY rust/ballista/format/Flight.proto /format

# Benchmark source
RUN mkdir -p /tmp/benchmarks/src
WORKDIR /tmp/benchmarks/
COPY rust/benchmarks/Cargo.* /tmp/benchmarks/
COPY rust/benchmarks/src/ /tmp/benchmarks/src/

RUN cargo build --release

# Copy the binary into a new container for a smaller docker image
FROM debian:buster-slim

COPY --from=build /tmp/benchmarks/target/release/ballista-benchmarks /
USER root

ENV RUST_LOG=info
ENV RUST_BACKTRACE=full

CMD ["/ballista-benchmarks"]
