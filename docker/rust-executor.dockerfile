FROM ballistacompute/rust-cached-deps:0.2.3 as build

# Compile Ballista
RUN rm -rf /tmp/ballista/src/
COPY rust/Cargo.* /tmp/ballista/
COPY rust/build.rs /tmp/ballista/
COPY rust/src/ /tmp/ballista/src/
COPY proto/ballista.proto /tmp/ballista/proto/

# workaround for Arrow 0.17.0 build issue
COPY rust/format/Flight.proto /format

RUN cargo build --release

# Copy the binary into a new container for a smaller docker image
FROM debian:buster-slim

COPY --from=build /tmp/ballista/target/release/executor /
USER root

ENV RUST_LOG=info
ENV RUST_BACKTRACE=full

CMD ["/executor"]
