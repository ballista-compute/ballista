FROM ballistacompute/rust-cached-deps:0.2.3 as build

# Compile Ballista
RUN rm -rf /tmp/ballista/src/
COPY rust/Cargo.* /tmp/ballista/
COPY rust/build.rs /tmp/ballista/
COPY rust/src/ /tmp/ballista/src/
COPY proto/ballista.proto /tmp/ballista/proto/

# workaround for Arrow 0.17.0 build issue
RUN mkdir /format
COPY rust/format/Flight.proto /format

RUN cargo build --release --target x86_64-unknown-linux-musl

## Copy the statically-linked binary into a scratch container.
FROM alpine:3.10

# Install Tini for better signal handling
RUN apk add --no-cache tini
ENTRYPOINT ["/sbin/tini", "--"]

COPY --from=build /tmp/ballista/target/x86_64-unknown-linux-musl/release/executor /
USER 1000

EXPOSE 9090

ENV RUST_LOG=info
ENV RUST_BACKTRACE=1

CMD ["/executor"]
