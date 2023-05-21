export S3DIS_TEST_MINIO_USER=minio
export S3DIS_TEST_MINIO_PASSWORD=$(cat config/minio_token)
mkdir -p cache
export S3DIS_TEST_CACHE_DIR=$(pwd)/cache
go test -race -v ./...