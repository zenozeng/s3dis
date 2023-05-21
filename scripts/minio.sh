#!/bin/bash

mkdir -p config
cat /proc/sys/kernel/random/uuid > config/minio_token

docker run -d -p 127.0.0.1:9000:9000 \
    -e MINIO_ROOT_USER="minio" \
    -e MINIO_ROOT_PASSWORD="$(cat config/minio_token)" \
    minio/minio:latest server /mnt/data
