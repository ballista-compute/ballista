FROM ballistacompute/rust-base:0.4.0-20210213 AS builder

# Fetch Ballista dependencies
COPY rust/ballista/Cargo.toml /tmp/ballista/
WORKDIR /tmp/ballista
RUN cargo fetch

# Compile Ballista dependencies
RUN mkdir -p /tmp/ballista/src/bin/ && echo 'fn main() {}' >> /tmp/ballista/src/bin/executor.rs
RUN mkdir -p /tmp/ballista/proto
COPY rust/ballista/proto/ballista.proto /tmp/ballista/proto/
COPY rust/ballista/build.rs /tmp/ballista/

ARG RELEASE_FLAG=--release
RUN cargo build $RELEASE_FLAG

#TODO relly need to copy whole project in, not just ballista crate, so we pick up the correct Cargo.lock
RUN rm -rf /tmp/ballista/Cargo.lock /tmp/ballista/src

COPY rust/ballista/Cargo.toml /tmp/ballista/
COPY rust/ballista/build.rs /tmp/ballista/
# for some reason, on some versions of docker, we hit this: https://github.com/moby/moby/issues/37965
# The suggested fix is to use this hack
RUN true
COPY rust/ballista/src/ /tmp/ballista/src/

RUN cargo build $RELEASE_FLAG

# put the executor on /executor (need to be copied from different places depending on FLAG)
ENV RELEASE_FLAG=${RELEASE_FLAG}
RUN if [ -z "$RELEASE_FLAG" ]; then mv /tmp/ballista/target/debug/executor /executor; else mv /tmp/ballista/target/release/executor /executor; fi

# put the executor on /executor (need to be copied from different places depending on FLAG)
ENV RELEASE_FLAG=${RELEASE_FLAG}
RUN if [ -z "$RELEASE_FLAG" ]; then mv /tmp/ballista/target/debug/scheduler /scheduler; else mv /tmp/ballista/target/release/scheduler /scheduler; fi

# Copy the binary into a new container for a smaller docker image
FROM ballistacompute/rust-base:0.4.0-20210213

COPY --from=builder /executor /

COPY --from=builder /scheduler /

ENV RUST_LOG=info
ENV RUST_BACKTRACE=full

CMD ["/executor", "--local"]
