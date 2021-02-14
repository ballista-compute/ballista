#!/bin/bash
set -e
./dev/build-rust.sh
pushd rust/benchmarks/tpch
./tpch-gen.sh

# hack to make benchmark and scheduler paths identical until
# https://github.com/ballista-compute/ballista/issues/473 is implemented
if [ ! -d "/data" ]
then
  echo "Attempting to create directory at /data"
  mkdir /data
fi
cp -f data/*.tbl /data/


docker-compose up -d
sleep 10
for query in 1 3 5 6 10 12
do
  cargo run benchmark --host localhost --port 50050 --query $query --path /data --format tbl --iterations 1
done
docker-compose down

popd
