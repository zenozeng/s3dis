package server

import (
	"github.com/zenozeng/s3dis/db"
	"github.com/zenozeng/s3dis/storage"
)

type Server struct {
	db *db.Database
}

type ServerConfig struct {
	CacheDir        string
	Singleton       bool
	MaxPartitionNum int
}

func NewServer(storage *storage.ObjectStorage, config *ServerConfig) *Server {
	return &Server{
		db: db.NewDatabase(storage, config.MaxPartitionNum, config.CacheDir, config.Singleton),
	}
}
