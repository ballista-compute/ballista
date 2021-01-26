#!/bin/bash

BALLISTA_VERSION=0.4.0-SNAPSHOT

#set -e

docker build -t ballistacompute/ballista-tpchgen:$BALLISTA_VERSION -f tpchgen.dockerfile .

# Generate data into the ./data directory
mkdir data 2>/dev/null
docker run -v `pwd`/data:/data -it ballistacompute/ballista-tpchgen:$BALLISTA_VERSION
ls -l data