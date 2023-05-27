package server

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

func TestStrings(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	key := uuid.NewString()
	val1 := uuid.NewString()
	val2 := uuid.NewString()
	err := server.Set(ctx, []byte(key), []byte(val1), nil)
	g.Expect(err).To(BeNil())
	val, err := server.Get(ctx, []byte(key))
	g.Expect(err).To(BeNil())
	g.Expect(string(val)).To(Equal(val1))
	err = server.Set(ctx, []byte(key), []byte(val2), nil)
	g.Expect(err).To(BeNil())
	val, err = server.Get(ctx, []byte(key))
	g.Expect(err).To(BeNil())
	g.Expect(string(val)).To(Equal(val2))
}
