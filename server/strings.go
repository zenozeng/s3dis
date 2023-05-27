package server

import (
	"context"
	"time"
)

func (c *Server) Set(ctx context.Context, key []byte, value []byte, exp *time.Time) error {
	return c.db.Set(ctx, []byte(key), func(prevVal []byte, prevExp *time.Time) ([]byte, *time.Time, error) {
		return value, exp, nil
	})
}

func (c *Server) Get(ctx context.Context, key []byte) ([]byte, error) {
	val, _, err := c.db.Get(ctx, []byte(key))
	if err != nil {
		return nil, err
	}
	return val, err
}
