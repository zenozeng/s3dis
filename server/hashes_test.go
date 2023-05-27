package server

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

func TestHashes(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	key := uuid.NewString()
	_, err := server.HGetAll(ctx, key)
	g.Expect(err).To(BeNil())
	v, err := server.HGet(ctx, key, "a")
	g.Expect(err).To(BeNil())
	g.Expect(v).To(Equal(""))
	err = server.HSet(ctx, key, "a", "A")
	g.Expect(err).To(BeNil())
	err = server.HSet(ctx, key, "b", "B")
	g.Expect(err).To(BeNil())
	v, err = server.HGet(ctx, key, "a")
	g.Expect(err).To(BeNil())
	g.Expect(v).To(Equal("A"))
	m, err := server.HGetAll(ctx, key)
	g.Expect(err).To(BeNil())
	g.Expect(m).To(Equal(map[string]string{"a": "A", "b": "B"}))
}

func TestHIncrBy(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	key := uuid.NewString()
	cnt, err := server.HIncrBy(ctx, key, "count", 10)
	g.Expect(err).To(BeNil())
	g.Expect(cnt).To(Equal(int64(10)))
	cnt, err = server.HIncrBy(ctx, key, "count", 10)
	g.Expect(err).To(BeNil())
	g.Expect(cnt).To(Equal(int64(20)))
}
