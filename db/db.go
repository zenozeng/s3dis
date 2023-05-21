package db

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/zenozeng/s3dis/storage"
	bolt "go.etcd.io/bbolt"
)

type Database struct {
	uuid            string
	storage         *storage.ObjectStorage
	partitions      sync.Map
	MaxPartitionNum int
	LocalDataDir    string
	Singleton       bool
}

func NewDatabase(storage *storage.ObjectStorage, MaxPartitionNum int, LocalDataDir string, Singleton bool) *Database {
	db := &Database{
		uuid:            uuid.NewString(),
		storage:         storage,
		MaxPartitionNum: MaxPartitionNum,
		LocalDataDir:    LocalDataDir,
		Singleton:       Singleton,
	}

	err := db.electLeader()
	if err != nil {
		panic(err)
	}
	return db
}

type Partition struct {
	rw   sync.RWMutex
	db   *bolt.DB
	path string // db path
	etag string
}

type Leader struct {
	UUID string `json:"uuid"`
}

func (db *Database) electLeader() error {
	if !db.Singleton {
		return nil
	}
	data, err := json.Marshal(&Leader{
		UUID: db.uuid,
	})
	if err != nil {
		return err
	}
	return db.storage.PutObject(context.Background(), "system/leader.json", data)
}

func (db *Database) getLeader() (*Leader, error) {
	leader := &Leader{}
	data, err := db.storage.GetObject(context.Background(), "system/leader.json")
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(data, leader)
	return leader, err
}

func (db *Database) getPartitionId(key []byte) string {
	partitionId := crc32.ChecksumIEEE(key) % uint32(db.MaxPartitionNum)
	return fmt.Sprintf("%d", partitionId)
}

// getBlotDB load local cached *bolt.DB or fetch from s3 if not found
func (db *Database) getPartition(partitionId string) (*Partition, error) {
	actual, _ := db.partitions.LoadOrStore(partitionId, &Partition{
		db:   nil,
		etag: "",
	})
	partition := actual.(*Partition)
	latestEtag, err := db.storage.GetEtag(context.Background(), fmt.Sprintf("partitions/%s/data.db", partitionId))
	if err != nil {
		return nil, err
	}

	if partition.etag == latestEtag && partition.db != nil {
		return partition, nil
	}

	partition.rw.Lock()
	defer partition.rw.Unlock()

	localDBPath := path.Join(db.LocalDataDir, fmt.Sprintf("%s-%d.db", partitionId, time.Now().UnixNano()))

	// Fetching latest db if etag not matching
	if partition.etag != latestEtag {
		obj, err := db.storage.Get(context.Background(), fmt.Sprintf("partitions/%s/data.db", partitionId), latestEtag)
		if err != nil {
			return nil, err
		}
		f, err := os.Create(localDBPath)
		if err != nil {
			return nil, err
		}
		_, err = io.Copy(f, obj)
		if err != nil {
			f.Close()
			return nil, err
		}
		err = f.Close()
		if err != nil {
			return nil, err
		}
	}

	boltDB, err := bolt.Open(localDBPath, 0600, &bolt.Options{
		ReadOnly: false,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open bolt db: %w", err)
	}
	err = db.prepare(boltDB)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare bolt db: %w", err)
	}
	partition.db = boltDB
	partition.path = localDBPath
	partition.etag = latestEtag
	return partition, nil
}

func (db *Database) prepare(boldDB *bolt.DB) error {
	return boldDB.Update(func(tx *bolt.Tx) error {
		systemBucket, err := tx.CreateBucketIfNotExists([]byte("system"))
		if err != nil {
			return err
		}
		version := systemBucket.Get([]byte("version"))
		if len(version) == 0 {
			version, err = db.migrateToV1(tx)
			if err != nil {
				return err
			}
		}
		if string(version) == "1" {
			return nil
		}
		panic(fmt.Errorf("unknown version: %s", version))
	})
}

// Migrate from v0 to v1
func (db *Database) migrateToV1(tx *bolt.Tx) ([]byte, error) {
	systemBucket, err := tx.CreateBucketIfNotExists([]byte("system"))
	if err != nil {
		return nil, err
	}
	_, err = tx.CreateBucketIfNotExists([]byte("value"))
	if err != nil {
		return nil, err
	}
	_, err = tx.CreateBucketIfNotExists([]byte("expiration"))
	if err != nil {
		return nil, err
	}
	newVersion := []byte("1")
	return newVersion, systemBucket.Put([]byte("version"), newVersion)
}

func (c *Database) view(partitionId string, fn func(tx *bolt.Tx) error) error {
	partition, err := c.getPartition(partitionId)
	if err != nil {
		return err
	}
	partition.rw.RLock()
	defer partition.rw.RUnlock()
	return partition.db.View(fn)
}

func (db *Database) update(partitionId string, fn func(tx *bolt.Tx) error) error {
	partition, err := db.getPartition(partitionId)
	if err != nil {
		return err
	}
	partition.rw.Lock()
	defer partition.rw.Unlock()
	err = partition.db.Update(fn)
	if err != nil {
		return err
	}
	file, err := os.Open(partition.path)
	if err != nil {
		return err
	}
	fi, err := file.Stat()
	if err != nil {
		return err
	}
	leader, err := db.getLeader()
	if err != nil {
		return err
	}
	if db.uuid != leader.UUID {
		return fmt.Errorf("leader changed leader.uuid=%s, db.uuid=%s", leader.UUID, db.uuid)
	}
	etag, err := db.storage.CompareAndSwap(
		context.Background(),
		fmt.Sprintf("partitions/%s/data.db", partitionId),
		file,
		fi.Size(),
		partition.etag,
	)
	if err != nil {
		return err
	}
	partition.etag = etag
	return nil
}

func (c *Database) Get(ctx context.Context, key []byte) ([]byte, *time.Time, error) {
	partitionId := c.getPartitionId(key)
	var val []byte
	var exp *time.Time
	err := c.view(partitionId, func(tx *bolt.Tx) error {
		valueBucket := tx.Bucket([]byte("value"))
		if valueBucket == nil {
			return nil
		}
		pxatBucket := tx.Bucket([]byte("expiration"))
		val = valueBucket.Get(key)
		pxat := pxatBucket.Get(key)
		if len(pxat) > 0 {
			unixMilli, err := strconv.ParseInt(string(pxat), 10, 64)
			if err != nil {
				return err
			}
			pxTime := time.UnixMilli(unixMilli)
			exp = &pxTime
			if !pxTime.After(time.Now()) {
				val = nil
			}
		}
		return nil
	})
	return val, exp, err
}

func (c *Database) incrStat(bucket *bolt.Bucket, key []byte, incrBy int64) error {
	v := bucket.Get(key)
	if len(v) == 0 {
		v = []byte("0")
	}
	n, err := strconv.ParseInt(string(v), 10, 64)
	if err != nil {
		return err
	}
	n += incrBy
	return bucket.Put(key, []byte(fmt.Sprintf("%d", n)))
}

// Set sets the value for a key.
//
// Buckets:
//
//	system: {
//	    version: "1",
//	    keys: "number of keys",
//	    expires: "number of keys with an expiration",
//	    total_write_commands_processed: "Total number of write commands processed by the server"
//	}
//
//	value: {
//		$key: $value
//	}
//
//	pxat: {
//	    $key: "Unix timestamp at which the key will expire, in milliseconds."
//	}
func (c *Database) Set(ctx context.Context, key []byte, w func([]byte, *time.Time) ([]byte, *time.Time, error)) error {
	partitionId := c.getPartitionId(key)
	return c.update(partitionId, func(tx *bolt.Tx) error {
		systemBucket, err := tx.CreateBucketIfNotExists([]byte("system"))
		if err != nil {
			return err
		}
		valueBucket, err := tx.CreateBucketIfNotExists([]byte("value"))
		if err != nil {
			return err
		}
		pxatBucket, err := tx.CreateBucketIfNotExists([]byte("expiration"))
		if err != nil {
			return err
		}
		prevVal := valueBucket.Get(key)
		prevPXAt := pxatBucket.Get(key)
		var prevExp *time.Time
		if len(prevPXAt) > 0 {
			unixMilli, err := strconv.ParseInt(string(prevPXAt), 10, 64)
			if err != nil {
				return err
			}
			prevExpTime := time.UnixMilli(unixMilli)
			prevExp = &prevExpTime
			if !prevExp.After(time.Now()) {
				prevVal = nil
			}
		}

		val, exp, err := w(prevVal, prevExp)
		if err != nil {
			return err
		}
		err = valueBucket.Put(key, val)
		if err != nil {
			return err
		}
		if exp != nil {
			err = pxatBucket.Put(key, []byte(fmt.Sprintf("%d", exp.UnixNano()/time.Millisecond.Nanoseconds())))
			if err != nil {
				return err
			}
		} else {
			err = pxatBucket.Delete(key)
			if err != nil {
				return err
			}
		}
		err = c.incrStat(systemBucket, []byte("total_write_commands_processed"), 1)
		if err != nil {
			return err
		}
		if len(prevVal) == 0 {
			err = c.incrStat(systemBucket, []byte("keys"), 1)
			if err != nil {
				return err
			}
			if exp != nil {
				err = c.incrStat(systemBucket, []byte("expires"), 1)
				if err != nil {
					return err
				}
			}
		} else {
			if prevExp != nil && exp == nil {
				err = c.incrStat(systemBucket, []byte("expires"), -1)
				if err != nil {
					return err
				}
			}
			if prevExp == nil && exp != nil {
				err = c.incrStat(systemBucket, []byte("expires"), 1)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
}
