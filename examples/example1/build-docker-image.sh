#!/usr/bin/env bash

set -e

mkdir -p temp/ballista/proto
cp ../../proto/ballista.proto temp/ballista/proto/

mkdir -p temp/ballista/rust
cp -rf ../../rust/* temp/ballista/rust

docker build -t ballista/example .



