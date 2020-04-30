FROM ballistacompute/rust-cached-deps as build

# Compile Ballista
RUN rm -rf /tmp/ballista/src/
COPY rust/Cargo.* /tmp/ballista/
COPY rust/build.rs /tmp/ballista/
COPY rust/src/ /tmp/ballista/src/
RUN cargo test
