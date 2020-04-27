# Ballista Development Guide

## Setting up a JVM development environment

### Install the Google Protocol Buffer compiler

```bash
wget https://github.com/protocolbuffers/protobuf/releases/download/v3.11.4/protobuf-all-3.11.4.tar.gz
tar xzf protobuf-all-3.11.4.tar.gz
cd protobuf-all-3.11.4
./configure
make
sudo make install
sudo ldconfig
```

## Setting up a Rust development environment

### Install OpenSSL

Follow instructions for [setting up OpenSSL](https://docs.rs/openssl/0.10.28/openssl/)






