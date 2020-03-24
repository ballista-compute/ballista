FROM ballistacompute/rust-cached-deps as build

# Compile Ballista
RUN rm -rf /tmp/ballista/src/
COPY rust/src/ /tmp/ballista/src/
RUN cargo build --release --target x86_64-unknown-linux-musl
RUN cargo build --release --target x86_64-unknown-linux-musl --examples

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
