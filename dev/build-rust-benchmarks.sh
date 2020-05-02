#!/usr/bin/env bash
set -e

docker build -t ballistacompute/rust-benchmarks:0.2.4-SNAPSHOT -f docker/rust-benchmarks.dockerfile .

#rm -rf temp 2>/dev/null
#mkdir -p temp/rust
#mkdir -p temp/proto
#cp -rf ../* temp/rust
#cp -rf ../../proto/* temp/proto
#docker build -t ballistacompute/rust-benchmarks:0.2.4-SNAPSHOT .