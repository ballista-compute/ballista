#!/bin/bash
docker build -t ballistacompute/rust-base -f docker/rust-base.dockerfile .
docker build -t ballistacompute/rust-cached-deps -f docker/rust-cached-deps.dockerfile .
docker build -t ballistacompute/rust -f docker/rust.dockerfile .