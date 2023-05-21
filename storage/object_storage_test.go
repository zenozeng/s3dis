package storage

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
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
	storage.MakeBucket(context.Background(), "test")
}

func TestGetEtagWithNonExistingPath(t *testing.T) {
	g := NewWithT(t)
	key := uuid.NewString()
	etag, err := storage.GetEtag(context.Background(), key)
	g.Expect(err).To(BeNil())
	g.Expect(etag).To(Equal(""))
}

func TestCreateNewObject(t *testing.T) {
	g := gomega.NewWithT(t)
	key := uuid.NewString()
	val := []byte(uuid.NewString())
	etag, err := storage.CompareAndSwap(context.Background(), key, bytes.NewReader(val), int64(len(val)), "")
	g.Expect(err).To(BeNil())
	g.Expect(etag).NotTo(BeEmpty())
	// check etag
	etag2, err := storage.GetEtag(context.Background(), key)
	g.Expect(err).To(BeNil())
	g.Expect(etag2).To(Equal(etag))
	// check value
	val2, err := storage.Get(context.Background(), key, etag)
	g.Expect(err).To(BeNil())
	buf := new(strings.Builder)
	_, err = io.Copy(buf, val2)
	g.Expect(err).To(BeNil())
	g.Expect([]byte(buf.String())).To(Equal(val))
}

func TestReplaceObject(t *testing.T) {
	g := gomega.NewWithT(t)
	key := uuid.NewString()
	val := []byte(uuid.NewString())
	etag, err := storage.CompareAndSwap(context.Background(), key, bytes.NewReader(val), int64(len(val)), "")
	g.Expect(err).To(BeNil())
	g.Expect(etag).NotTo(BeEmpty())
	val = []byte(uuid.NewString())

	// CompareAndSwap
	etag2, err := storage.CompareAndSwap(context.Background(), key, bytes.NewReader(val), int64(len(val)), etag)
	g.Expect(err).To(BeNil())
	g.Expect(etag2).NotTo(Equal(etag))

	// CompareAndSwap should throw error if etag not match
	_, err = storage.CompareAndSwap(context.Background(), key, bytes.NewReader(val), int64(len(val)), etag)
	g.Expect(err).NotTo(BeNil())
	etag3, err := storage.GetEtag(context.Background(), key)
	g.Expect(err).To(BeNil())
	g.Expect(etag3).To(Equal(etag2))
}

func TestGetObjectWithExpiredEtag(t *testing.T) {
	g := NewWithT(t)
	key := uuid.NewString()
	val := []byte(uuid.NewString())
	etag, err := storage.CompareAndSwap(context.Background(), key, bytes.NewReader(val), int64(len(val)), "")
	g.Expect(err).To(BeNil())
	g.Expect(etag).NotTo(BeEmpty())
	val = []byte(uuid.NewString())
	etag2, err := storage.CompareAndSwap(context.Background(), key, bytes.NewReader(val), int64(len(val)), etag)
	g.Expect(err).To(BeNil())
	g.Expect(etag2).NotTo(Equal(etag))

	// GetObject using expired etag should return PreconditionFailed error
	obj, err := storage.Get(context.Background(), key, etag)
	g.Expect(err).To(BeNil())
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(obj)
	g.Expect(err).NotTo(BeNil())
	g.Expect(err.(minio.ErrorResponse).Code).To(Equal("PreconditionFailed"))
}
