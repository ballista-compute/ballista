# Setting up a Rust development environment

You will need a standard Rust development environment. The easiest way to achieve this is by using rustup: https://rustup.rs/

## Install OpenSSL

Follow instructions for [setting up OpenSSL](https://docs.rs/openssl/0.10.28/openssl/). For Ubuntu users, the following 
command works.

```bash
sudo apt-get install pkg-config libssl-dev
```

## Install CMake

You'll need cmake in order to compile some of ballista's dependencies. Ubuntu users can use the following command:

```bash
sudo apt-get install cmake
```