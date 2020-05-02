#!/usr/bin/env bash
set -e
rm -rf temp 2>/dev/null
mkdir -p temp/rust
mkdir -p temp/proto
cp -rf ../../rust/* temp/rust
cp -rf ../../proto/* temp/proto
docker build -t ballistacompute/benchmarks:0.2.4-SNAPSHOT .