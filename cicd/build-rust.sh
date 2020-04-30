#!/bin/bash

# run from root:
# ./cicd/build-rust.sh

set -e
docker build -t ballistacompute/ci-build-rust -f cicd/ci-build-rust.dockerfile .
