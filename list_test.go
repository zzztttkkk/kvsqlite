package kvsqlite

import (
	"context"
	"fmt"
	"testing"
)

func TestList(t *testing.T) {
	defer db.Close()

	db.Scope(context.Background(), func(ctx context.Context, tx Tx) error {
		for val, err := range tx.List("eles").DescSeq(ctx, 10) {
			if err != nil {
				return err
			}
			fmt.Println(val)
		}
		return nil
	})
}
