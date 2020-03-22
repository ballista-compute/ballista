FROM ballistacompute/rust-cached-deps as build

WORKDIR /tmp/ballista

ADD main.rs /tmp/ballista/examples/parallel-aggregate/

RUN cargo build --examples --target x86_64-unknown-linux-musl --release
