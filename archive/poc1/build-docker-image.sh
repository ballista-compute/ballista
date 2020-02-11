#!/usr/bin/env bash

REPO=andygrove

docker build -t ${REPO}/ballista-platform -f docker/Dockerfile.platform .
docker build -t ${REPO}/ballista -f docker/Dockerfile.binary --build-arg REPO=${REPO} .
