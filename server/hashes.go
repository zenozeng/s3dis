package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

type Hash struct {
	APIVersion string            `json:"apiVersion"`
	Value      map[string]string `json:"value"`
}

func (c *Server) HSet(ctx context.Context, key string, field string, value string) error {
	return c.db.Set(ctx, []byte(key), func(prevVal []byte, prevExp *time.Time) ([]byte, *time.Time, error) {
		hash := &Hash{
			APIVersion: "v1",
			Value:      map[string]string{},
		}
		if len(prevVal) > 0 {
			err := json.Unmarshal(prevVal, hash)
			if err != nil {
				return nil, nil, err
			}
		}
		if hash.Value == nil {
			hash.Value = map[string]string{}
		}
		hash.Value[field] = value
		val, err := json.Marshal(hash)
		return val, prevExp, err
	})
}

func (c *Server) HGet(ctx context.Context, key string, field string) (string, error) {
	hash, err := c.HGetAll(ctx, key)
	if err != nil {
		return "", err
	}
	return hash[field], nil
}

func (c *Server) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	hash := &Hash{
		APIVersion: "v1",
		Value:      map[string]string{},
	}
	data, _, err := c.db.Get(ctx, []byte(key))
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		data = []byte("{}")
	}
	err = json.Unmarshal(data, hash)
	if err != nil {
		return nil, err
	}
	if hash.Value == nil {
		return map[string]string{}, nil
	}
	return hash.Value, nil
}

func (c *Server) HIncrBy(ctx context.Context, key string, field string, increment int64) (int64, error) {
	num := int64(0)
	err := c.db.Set(ctx, []byte(key), func(prevVal []byte, prevExp *time.Time) ([]byte, *time.Time, error) {
		hash := &Hash{
			APIVersion: "v1",
			Value:      map[string]string{},
		}
		if len(prevVal) > 0 {
			err := json.Unmarshal(prevVal, hash)
			if err != nil {
				return nil, nil, err
			}
		}
		if hash.Value == nil {
			hash.Value = map[string]string{}
		}

		val := hash.Value[field]
		if val == "" {
			val = "0"
		}
		parsed, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return nil, nil, err
		}
		num = parsed + increment
		hash.Value[field] = fmt.Sprintf("%d", num)

		data, err := json.Marshal(hash)
		return data, prevExp, err
	})
	return num, err
}
