#!/usr/bin/env bash

set -e

mkdir -p temp/proto
cp ../proto/ballista.proto temp/proto/

docker build -t ballista/rust .



