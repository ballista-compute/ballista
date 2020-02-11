ARG REPO=andygrove

FROM $REPO/ballista-platform as build

WORKDIR /tmp/ballista/examples
RUN USER=root cargo new nyctaxi
WORKDIR /tmp/ballista/examples/nyctaxi
COPY Cargo.toml ./
COPY Cargo.lock ./
RUN cargo fetch
RUN cargo build --release --target x86_64-unknown-linux-musl

# build the real project
COPY src ./src
RUN cargo build --release --target x86_64-unknown-linux-musl

# Copy the statically-linked binary into a scratch container.
FROM alpine
COPY --from=build /tmp/ballista/examples/nyctaxi/target/x86_64-unknown-linux-musl/release/ballista-nyctaxi /

ENV RUST_LOG=info
ENV RUST_BACKTRACE=1

WORKDIR /tmp

RUN apk update && \
    apk upgrade && \
    apk add bash

CMD ["/ballista-nyctaxi"]
