FROM ballistacompute/rust-cached-deps:0.2.3 as build

# Ballista Rust source code (as dependency)
RUN mkdir -p /tmp/ballista/rust/src
ADD temp/rust/src /tmp/ballista/rust/src/
ADD temp/rust/Cargo.toml /tmp/ballista/rust/
ADD temp/rust/build.rs /tmp/ballista/rust/

# protobuf needs to be copied to two different places
ADD temp/proto /tmp/ballista/rust/proto/
ADD temp/proto /tmp/ballista/proto/

# Benchmark source
RUN mkdir -p /tmp/ballista/benchmarks/rust/src
WORKDIR /tmp/ballista/benchmarks/rust
ADD src /tmp/ballista/benchmarks/rust/src/
ADD Cargo.toml /tmp/ballista/benchmarks/rust/

# Build
RUN cargo build --target x86_64-unknown-linux-musl --release

# Copy the statically-linked binary into a scratch container.
FROM alpine:3.10

# Install Tini for better signal handling
RUN apk add --no-cache tini
ENTRYPOINT ["/sbin/tini", "--"]

COPY --from=build /tmp/ballista/benchmarks/rust/target/x86_64-unknown-linux-musl/release/ballista-benchmarks /
USER 1000

EXPOSE 9090

ENV RUST_LOG=info
ENV RUST_BACKTRACE=full

CMD ["/ballista-benchmarks"]
