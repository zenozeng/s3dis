package db

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/onsi/gomega"
	"github.com/zenozeng/s3dis/storage"
)

var (
	objectStorage *storage.ObjectStorage
	db            *Database
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
	db = NewDatabase(objectStorage, 1024, os.Getenv("S3DIS_TEST_CACHE_DIR"), true)
}

func TestReopenDB(t *testing.T) {
	g := NewWithT(t)
	key := []byte(uuid.NewString())
	val := []byte(uuid.NewString())
	err := db.Set(context.Background(), key, func(b []byte, exp *time.Time) ([]byte, *time.Time, error) {
		return val, nil, nil
	})
	g.Expect(err).To(BeNil())
	// reset db.partitions[partitionID]
	partitionId := db.getPartitionId(key)
	db.partitions.Delete(partitionId)
	// reopen db and read key again
	res, _, err := db.Get(context.Background(), key)
	g.Expect(err).To(BeNil())
	g.Expect(res).To(Equal(val))
	// set again
	val2 := []byte(uuid.NewString())
	err = db.Set(context.Background(), key, func(b []byte, exp *time.Time) ([]byte, *time.Time, error) {
		return val2, nil, nil
	})
	g.Expect(err).To(BeNil())
	res, _, err = db.Get(context.Background(), key)
	g.Expect(err).To(BeNil())
	g.Expect(res).To(Equal(val2))
}

func TestLeaderChanged(t *testing.T) {
	g := NewWithT(t)

	prevDB := NewDatabase(objectStorage, 1024, os.Getenv("S3DIS_TEST_CACHE_DIR"), true)
	key := []byte(uuid.NewString())
	val := []byte(uuid.NewString())
	err := prevDB.Set(context.Background(), key, func(b []byte, exp *time.Time) ([]byte, *time.Time, error) {
		return val, nil, nil
	})
	g.Expect(err).To(BeNil())

	// leader changed
	db = NewDatabase(objectStorage, 1024, os.Getenv("S3DIS_TEST_CACHE_DIR"), true)

	// prev leader should not be able to write
	val2 := []byte(uuid.NewString())
	err = prevDB.Set(context.Background(), key, func(b []byte, exp *time.Time) ([]byte, *time.Time, error) {
		return val2, nil, nil
	})
	g.Expect(err).NotTo(BeNil())
	g.Expect(strings.HasPrefix(err.Error(), "leader changed")).To(Equal(true))

	// res should be val
	res, _, err := db.Get(context.Background(), key)
	g.Expect(err).To(BeNil())
	g.Expect(res).To(Equal(val))
}

func TestInfo(t *testing.T) {
	g := NewWithT(t)
	info, err := db.Info(context.Background())
	g.Expect(err).To(BeNil())
	g.Expect(info).NotTo(BeNil())
	err = db.Set(context.Background(), []byte(uuid.NewString()), func(b []byte, exp *time.Time) ([]byte, *time.Time, error) {
		return []byte(uuid.NewString()), nil, nil
	})
	g.Expect(err).To(BeNil())
	info2, err := db.Info(context.Background())
	g.Expect(err).To(BeNil())
	g.Expect(info2.Keys).To(Equal(info.Keys + 1))
	g.Expect(info2.TotalWriteCommandsProcessed).To(Equal(info.TotalWriteCommandsProcessed + 1))
}