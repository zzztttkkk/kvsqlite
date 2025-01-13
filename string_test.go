package kvsqlite

import (
	"context"
	"fmt"
	"testing"
)

var db *DB

func init() {
	var err error
	db, err = OpenDB(context.Background(), "./kv.db")
	if err != nil {
		panic(err)
	}
}

func TestStringHandle(t *testing.T) {
	defer db.Close()

	err := db.Scope(context.Background(), func(ctx context.Context, tx Tx) error {
		fmt.Println(tx.String("aaa").Set(ctx, String("1w2")))
		fmt.Println(tx.String("xxx").Incr(ctx, -7))
		return nil
	})
	fmt.Println(err)
}
