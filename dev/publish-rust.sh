#!/bin/bash

set -e

pushd rust/lib
cargo publish
popd

