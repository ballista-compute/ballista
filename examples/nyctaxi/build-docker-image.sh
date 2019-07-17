#!/usr/bin/env bash

REPO=andygrove

docker pull andygrove/ballista-platform
docker build -t ${REPO}/example-nyctaxi --build-arg REPO=${REPO} .
