package server

import (
	"context"
	"os"

	"github.com/zenozeng/s3dis/storage"

	"github.com/onsi/gomega"
)

var (
	objectStorage *storage.ObjectStorage
	server        *Server
	NewWithT      = gomega.NewWithT
	Equal         = gomega.Equal
	BeNil         = gomega.BeNil
	BeEmpty       = gomega.BeEmpty
)

func init() {
	objectStorage = storage.NewObjectStorage(&storage.ObjectStorageConfig{
		Endpoint:        "127.0.0.1:9000",
		AccessKeyID:     os.Getenv("S3DIS_TEST_MINIO_USER"),
		SecretAccessKey: os.Getenv("S3DIS_TEST_MINIO_PASSWORD"),
		UseSSL:          false,
		Bucket:          "test",
		PathPrefix:      "test-prefix",
	})
	objectStorage.MakeBucket(context.Background(), "test")
	server = NewServer(objectStorage, &ServerConfig{
		CacheDir:  os.Getenv("S3DIS_TEST_CACHE_DIR"),
		Singleton: true,
		MaxPartitionNum: 1024,
	})
}
