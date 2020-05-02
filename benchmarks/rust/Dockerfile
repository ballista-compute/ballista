FROM ballistacompute/rust-cached-deps:0.2.3 as build

RUN mkdir -p /tmp/ballista/benchmarks/src
WORKDIR /tmp/ballista/benchmarks

ADD src /tmp/ballista/benchmarks/src/
ADD Cargo.toml /tmp/ballista/benchmarks/

RUN cargo build --target x86_64-unknown-linux-musl --release

# Copy the statically-linked binary into a scratch container.
FROM alpine:3.10

# Install Tini for better signal handling
RUN apk add --no-cache tini
ENTRYPOINT ["/sbin/tini", "--"]

COPY --from=build /tmp/ballista/benchmarks/target/x86_64-unknown-linux-musl/release/ballista-benchmarks /
USER 1000

EXPOSE 9090

ENV RUST_LOG=info
ENV RUST_BACKTRACE=full

CMD ["/ballista-benchmarks"]
