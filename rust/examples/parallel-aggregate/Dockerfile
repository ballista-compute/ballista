FROM ballistacompute/rust-cached-deps as build

WORKDIR /tmp/ballista

ADD main.rs /tmp/ballista/examples/parallel-aggregate/

RUN cargo build --examples --target x86_64-unknown-linux-musl --release

# Copy the statically-linked binary into a scratch container.
FROM alpine:3.10

# Install Tini for better signal handling
RUN apk add --no-cache tini
ENTRYPOINT ["/sbin/tini", "--"]

COPY --from=build /tmp/ballista/target/x86_64-unknown-linux-musl/release/examples/parallel-aggregate /
USER 1000

EXPOSE 9090

ENV RUST_LOG=info
ENV RUST_BACKTRACE=1

CMD ["/parallel-aggregate"]
