#!/bin/bash

echo "Waiting MinIO to launch on :9000"

while ! timeout 1 bash -c "echo > /dev/tcp/localhost/9000"; do
    sleep 1
done

echo "MinIO launched"

export S3DIS_TEST_MINIO_USER=minio
export S3DIS_TEST_MINIO_PASSWORD=$(cat config/minio_token)
mkdir -p cache
export S3DIS_TEST_CACHE_DIR=$(pwd)/cache
go test -p 1 -race -v ./...