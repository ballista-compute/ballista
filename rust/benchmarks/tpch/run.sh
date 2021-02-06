#!/usr/bin/env bash

# Start Ballista executors
docker-compose up -d

# Wait for executors to start
sleep 20

# Run integration tests
cargo run benchmark --host localhost --port 50051 --query 12 --path data --format tbl
#cargo run benchmark --host localhost --port 50052 --query 12 --path data --format tbl
#cargo run benchmark --host localhost --port 50053 --query 12 --path data --format tbl

# Stop Ballista servers
docker-compose down
