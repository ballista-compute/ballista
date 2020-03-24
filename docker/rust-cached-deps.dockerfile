FROM ballistacompute/rust-base as build

# Fetch Ballista dependencies
WORKDIR /tmp
RUN USER=root cargo new ballista --lib
WORKDIR /tmp/ballista
COPY rust/Cargo.toml rust/Cargo.lock /tmp/ballista/
RUN cargo fetch

# Compile Ballista dependencies
RUN mkdir -p /tmp/ballista/src/bin/ && echo 'fn main() {}' >> /tmp/ballista/src/bin/executor.rs
COPY proto /tmp/proto/
COPY rust/build.rs /tmp/ballista/
RUN cargo build --release --target x86_64-unknown-linux-musl

# Compile Ballista
COPY rust/src/ /tmp/ballista/src/
RUN cargo build --release --target x86_64-unknown-linux-musl
RUN cargo build --release --target x86_64-unknown-linux-musl --examples

