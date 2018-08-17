#!/bin/bash
set -e

TAG=$1

echo "$DOCKER_PASSWORD" | docker login -u "$DOCKER_USERNAME" --password-stdin

docker build -t blackfynn/python-client-test:$TAG .
docker push blackfynn/python-client-test
