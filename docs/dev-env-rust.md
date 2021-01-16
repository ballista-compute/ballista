# Setting up a Rust development environment

## Install OpenSSL

Follow instructions for [setting up OpenSSL](https://docs.rs/openssl/0.10.28/openssl/). For Ubuntu users, the following 
command works.

```bash
sudo apt-get install pkg-config libssl-dev
```


## Install Protobuf

Use following instructions to install the protobuf compiler on Ubuntu or similar Linux platforms.

```
$ wget https://github.com/protocolbuffers/protobuf/releases/download/v3.11.4/protobuf-all-3.11.4.tar.gz
$ tar xzf protobuf-all-3.11.4.tar.gz
$ cd protobuf-3.11.4
$ ./configure
$ make
$ sudo make install
$ sudo ldconfig
```

MacOS user can install with `brew`

```
$ brew install protobuf
```

