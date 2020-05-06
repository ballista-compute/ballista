FROM ballistacompute/rust-cached-deps:0.2.3 as build

# Add Ballista
RUN rm -rf /tmp/ballista/src/
COPY rust/Cargo.* /tmp/ballista/
COPY rust/build.rs /tmp/ballista/
COPY rust/src/ /tmp/ballista/src/
COPY proto/ballista.proto /tmp/ballista/proto/

# workaround for Arrow 0.17.0 build issue
COPY rust/format/Flight.proto /format

# Benchmark source
RUN mkdir -p /tmp/ballista/benchmarks/src
WORKDIR /tmp/ballista/benchmarks/
COPY rust/benchmarks/Cargo.* /tmp/ballista/benchmarks/
COPY rust/benchmarks/src/ /tmp/ballista/benchmarks/src/

RUN cargo build --release

# Copy the binary into a new container for a smaller docker image
FROM debian:buster-slim

COPY --from=build /tmp/ballista/benchmarks/target/release/ballista-benchmarks /
USER root

ENV RUST_LOG=info
ENV RUST_BACKTRACE=full

CMD ["/ballista-benchmarks"]
