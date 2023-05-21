package storage

import (
	"context"
	"os"

	"github.com/minio/minio-go/v7"

	"github.com/onsi/gomega"
)

var (
	storage  *ObjectStorage
	NewWithT = gomega.NewWithT
	Equal    = gomega.Equal
	BeNil    = gomega.BeNil
	BeEmpty  = gomega.BeEmpty
)

func init() {
	storage = NewObjectStorage(&ObjectStorageConfig{
		Endpoint:        "127.0.0.1:9000",
		AccessKeyID:     os.Getenv("S3DIS_TEST_MINIO_USER"),
		SecretAccessKey: os.Getenv("S3DIS_TEST_MINIO_PASSWORD"),
		UseSSL:          false,
		Bucket:          "test",
		PathPrefix:      "test-prefix",
	})
	storage.minioClient.MakeBucket(context.Background(), "test", minio.MakeBucketOptions{})
}
