#!/bin/bash

set -e

pushd rust
cargo test --release
popd 

pushd jvm
./gradlew assemble
popd

docker build -t ballista/jvm jvm/executor
docker build -t ballista/rust rust
