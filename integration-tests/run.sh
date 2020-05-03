#!/usr/bin/env bash

# Start Ballista executors
docker-compose up -d

# Wait for executors to start
#sleep 10

# Run integration tests
pushd rust
cargo run
popd

# Stop Ballista servers
docker-compose down
