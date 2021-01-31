# Development Environment

## Setting up a JVM development environment

### Install the Google Protocol Buffer compiler

The gradle build script uses the [protobuf-gradle-plugin](https://github.com/google/protobuf-gradle-plugin) Gradle plugin to generate Java source code from the Ballista protobuf file and this depends on the protobuf compiler being installed.

Use the following instructions to install the protobuf compiler on Ubuntu or similar Linux platforms. 

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

Follow instructions for [setting up OpenSSL](https://docs.rs/openssl/0.10.28/openssl/). For Ubuntu users, the following command works.

```bash
sudo apt-get install pkg-config libssl-dev
```






