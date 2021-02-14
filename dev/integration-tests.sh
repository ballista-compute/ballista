#!/bin/bash
set -e
./dev/build-rust.sh
pushd rust/benchmarks/tpch
./tpch-gen.sh

docker-compose up -d
docker-compose run ballista-client ./run.sh
docker-compose down

popd
