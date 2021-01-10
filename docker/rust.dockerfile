# Base image extends debian:buster-slim
FROM rust:1.45.0-buster AS builder

RUN apt update && apt -y install musl musl-dev musl-tools libssl-dev openssl

#NOTE: the following was copied from https://github.com/emk/rust-musl-builder/blob/master/Dockerfile under Apache 2.0 license

# The OpenSSL version to use. We parameterize this because many Rust
# projects will fail to build with 1.1.
#ARG OPENSSL_VERSION=1.0.2r
ARG OPENSSL_VERSION=1.1.1b

# Build a static library version of OpenSSL using musl-libc.  This is needed by
# the popular Rust `hyper` crate.
#
# We point /usr/local/musl/include/linux at some Linux kernel headers (not
# necessarily the right ones) in an effort to compile OpenSSL 1.1's "engine"
# component. It's possible that this will cause bizarre and terrible things to
# happen. There may be "sanitized" header
RUN echo "Building OpenSSL" && \
    ls /usr/include/linux && \
    mkdir -p /usr/local/musl/include && \
    ln -s /usr/include/linux /usr/local/musl/include/linux && \
    ln -s /usr/include/x86_64-linux-gnu/asm /usr/local/musl/include/asm && \
    ln -s /usr/include/asm-generic /usr/local/musl/include/asm-generic && \
    cd /tmp && \
    curl -LO "https://www.openssl.org/source/openssl-$OPENSSL_VERSION.tar.gz" && \
    tar xvzf "openssl-$OPENSSL_VERSION.tar.gz" && cd "openssl-$OPENSSL_VERSION" && \
    env CC=musl-gcc ./Configure no-shared no-zlib -fPIC --prefix=/usr/local/musl -DOPENSSL_NO_SECURE_MEMORY linux-x86_64 && \
    env C_INCLUDE_PATH=/usr/local/musl/include/ make depend && \
    env C_INCLUDE_PATH=/usr/local/musl/include/ make && \
    make install && \
    rm /usr/local/musl/include/linux /usr/local/musl/include/asm /usr/local/musl/include/asm-generic && \
    rm -r /tmp/*

RUN echo "Building zlib" && \
    cd /tmp && \
    ZLIB_VERSION=1.2.11 && \
    curl -LO "http://zlib.net/zlib-$ZLIB_VERSION.tar.gz" && \
    tar xzf "zlib-$ZLIB_VERSION.tar.gz" && cd "zlib-$ZLIB_VERSION" && \
    CC=musl-gcc ./configure --static --prefix=/usr/local/musl && \
    make && make install && \
    rm -r /tmp/*

RUN echo "Building libpq" && \
    cd /tmp && \
    POSTGRESQL_VERSION=11.2 && \
    curl -LO "https://ftp.postgresql.org/pub/source/v$POSTGRESQL_VERSION/postgresql-$POSTGRESQL_VERSION.tar.gz" && \
    tar xzf "postgresql-$POSTGRESQL_VERSION.tar.gz" && cd "postgresql-$POSTGRESQL_VERSION" && \
    CC=musl-gcc CPPFLAGS=-I/usr/local/musl/include LDFLAGS=-L/usr/local/musl/lib ./configure --with-openssl --without-readline --prefix=/usr/local/musl && \
    cd src/interfaces/libpq && make all-static-lib && make install-lib-static && \
    cd ../../bin/pg_config && make && make install && \
    rm -r /tmp/*

ENV OPENSSL_DIR=/usr/local/musl/ \
    OPENSSL_INCLUDE_DIR=/usr/local/musl/include/ \
    DEP_OPENSSL_INCLUDE=/usr/local/musl/include/ \
    OPENSSL_LIB_DIR=/usr/local/musl/lib/ \
    OPENSSL_STATIC=1 \
    PQ_LIB_STATIC_X86_64_UNKNOWN_LINUX_MUSL=1 \
    PG_CONFIG_X86_64_UNKNOWN_LINUX_GNU=/usr/bin/pg_config \
    PKG_CONFIG_ALLOW_CROSS=true \
    PKG_CONFIG_ALL_STATIC=true \
    LIBZ_SYS_STATIC=1 \
    TARGET=musl

# The content copied mentioned in the NOTE above ends here.

## Download the target for static linking.
RUN rustup target add x86_64-unknown-linux-musl
RUN cargo install cargo-build-deps

# Fetch Ballista dependencies
COPY rust/ballista/Cargo.toml /tmp/ballista/
WORKDIR /tmp/ballista
RUN cargo fetch

# Compile Ballista dependencies
RUN mkdir -p /tmp/ballista/src/bin/ && echo 'fn main() {}' >> /tmp/ballista/src/bin/executor.rs
RUN mkdir -p /tmp/ballista/proto
COPY proto/ballista.proto /tmp/ballista/proto/
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

# Copy the binary into a new container for a smaller docker image
FROM debian:buster-slim

COPY --from=builder /executor /

ENV RUST_LOG=info
ENV RUST_BACKTRACE=full

CMD ["/executor"]
