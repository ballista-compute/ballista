# Ballista Development Guide

## Setting up a JVM build environment

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

## Setting up a Rust build environment

### Install OpenSSL

Follow instructions for [setting up OpenSSL](https://docs.rs/openssl/0.10.28/openssl/)



# Ballista Docker Images

Pre-built docker images are available from [Docker Hub](https://hub.docker.com/orgs/ballistacompute/repositories) but here are the commands to build the images from source.

Run these commands from the root directory of the project.

```bash
./dev/build-docker-images.sh
```

## Publishing JVM Artifacts

https://help.github.com/en/github/authenticating-to-github/generating-a-new-gpg-key
gpg --full-generate-key
gpg --export-secret-keys > ~/.gnupg/secring.gpg
gpg --send-keys <KEYID> --key-server keys.openpgp.org
