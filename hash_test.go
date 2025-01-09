package kvsqlite

import (
	"context"
	"testing"
)

func TestHashHandle(t *testing.T) {
	defer db.Close()

	db.Scope(context.Background(), func(ctx context.Context, tx Tx) error {
		return nil
	})
}
