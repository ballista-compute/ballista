#!/bin/bash
./dev/build-rust.sh
pushd rust/benchmarks/tpch
./tpch-gen.sh

# hack to make benchmark and scheduler paths identical until
# https://github.com/ballista-compute/ballista/issues/473 is implemented
mkdir /data 2>/dev/null
cp data/*.tbl /data

docker-compose up -d
sleep 10
cargo run benchmark --host localhost --port 50050 --query 12 --path /data --format tbl --iterations 1
docker-compose down

popd
