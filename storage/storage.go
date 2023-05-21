package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type ObjectStorage struct {
	bucket      string
	pathPrefix  string
	minioClient *minio.Client
}

type ObjectStorageConfig struct {
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
	UseSSL          bool
	Bucket          string
	PathPrefix      string
}

func NewObjectStorage(config *ObjectStorageConfig) *ObjectStorage {
	client, err := minio.New(config.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.AccessKeyID, config.SecretAccessKey, ""),
		Secure: config.UseSSL,
	})
	if err != nil {
		panic(err)
	}
	return &ObjectStorage{
		bucket:      config.Bucket,
		pathPrefix:  config.PathPrefix,
		minioClient: client,
	}
}

func (s *ObjectStorage) GetEtag(ctx context.Context, objectPath string) (string, error) {
	obj, err := s.minioClient.GetObject(ctx, s.bucket, path.Join(s.pathPrefix, objectPath), minio.GetObjectOptions{})
	if err != nil {
		return "", err
	}
	stat, err := obj.Stat()
	if err != nil {
		if err, ok := err.(minio.ErrorResponse); ok {
			if err.Code == "NoSuchKey" {
				return "", nil
			}
		}
		return "", err
	}
	return stat.ETag, nil
}

func (s *ObjectStorage) Get(ctx context.Context, objectPath string, etag string) (io.Reader, error) {
	readOpts := minio.GetObjectOptions{}
	err := readOpts.SetMatchETag(etag)
	if err != nil {
		return nil, err
	}
	obj, err := s.minioClient.GetObject(ctx, s.bucket, path.Join(s.pathPrefix, objectPath), readOpts)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

// CompareAndSwap try to compare and swap in best effort fashion
// Known limitations:
// - S3 does not provide `if-match: etag` for PutObject
func (s *ObjectStorage) CompareAndSwap(ctx context.Context, objectPath string, newValue io.Reader, newLength int64, oldEtag string) (string, error) {
	latestEtag, err := s.GetEtag(ctx, objectPath)
	if err != nil {
		return "", err
	}
	if latestEtag != oldEtag {
		return "", fmt.Errorf("etag not match got: %s, expected: %s", latestEtag, oldEtag)
	}
	info, err := s.minioClient.PutObject(ctx, s.bucket, path.Join(s.pathPrefix, objectPath), newValue, newLength, minio.PutObjectOptions{})
	if err != nil {
		return "", err
	}
	return info.ETag, nil
}

func (s *ObjectStorage) GetObject(ctx context.Context, objectPath string) ([]byte, error) {
	obj, err := s.minioClient.GetObject(ctx, s.bucket, path.Join(s.pathPrefix, objectPath), minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	_, err = io.Copy(&buf, obj)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (s *ObjectStorage) PutObject(ctx context.Context, objectPath string, value []byte) error {
	_, err := s.minioClient.PutObject(ctx, s.bucket, path.Join(s.pathPrefix, objectPath), bytes.NewReader(value), int64(len(value)), minio.PutObjectOptions{})
	return err
}
