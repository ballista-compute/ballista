#!/bin/bash

set -e

pushd rust
cargo build --release
popd 

pushd jvm
./gradlew assemble
popd

docker build -t ballista/executor-kotlin jvm/executor
