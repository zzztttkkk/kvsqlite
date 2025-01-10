package kvsqlite

import (
	"context"
	"fmt"
	"testing"
)

func TestHashHandle(t *testing.T) {
	defer db.Close()

	db.Scope(context.Background(), func(ctx context.Context, tx Tx) error {
		tx.Hash("ccc").Set(ctx, "a", "121")
		fmt.Println(tx.Hash("ccc").GetAll(ctx))

		tx.Hash("xxxx").SetAll(ctx, map[string]string{"xxx": "dd", "yyy": "ll"})
		fmt.Println(tx.Hash("xxxx").GetAll(ctx))
		fmt.Println(tx.Hash("xxxx").Size(ctx))
		return nil
	})
}
