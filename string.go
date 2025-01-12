package kvsqlite

import (
	"context"
	"database/sql"
	"fmt"
)

type _StringHandle struct {
	tx *Tx
}

func (tx *Tx) String() _StringHandle {
	return _StringHandle{tx: tx}
}

func (handle _StringHandle) ensurekind(ctx context.Context, key string) error {
	return handle.tx.ensurekind(ctx, KeyKindString, key)
}

func (handle _StringHandle) Get(ctx context.Context, key string) (Value, error) {
	row := handle.tx.queryone(ctx, `select value from kv_string where key = ?`, key)
	err := row.Err()
	if err != nil {
		return Value{}, err
	}
	var val Value
	err = row.Scan(&val)
	return val, err
}

func (handle _StringHandle) Set(ctx context.Context, key string, val Value) error {
	err := handle.ensurekind(ctx, key)
	if err != nil {
		if err != sql.ErrNoRows {
			return err
		}
		err = handle.tx.addkey(ctx, key, KeyKindString)
		if err != nil {
			return fmt.Errorf("kvsqlite: add key failed, %s", err)
		}
	}
	_, err = handle.tx.exec(ctx, `insert or replace into kv_string (key, value) values (?, ?)`, key, val)
	return err
}

func (handle _StringHandle) delone(ctx context.Context, key string) (int64, error) {
	return handle.tx.exec(ctx, `delete from kv_string where key = ?`, key)
}

func (handle _StringHandle) Incr(
	ctx context.Context, key string, amount int64,
) (
	int64, error,
) {
	pv, err := handle.Get(ctx, key)
	if err != nil {
		if err == sql.ErrNoRows {
			return amount, handle.Set(ctx, key, Int(amount))
		}
		return 0, err
	}
	iv, err := pv.Int64()
	if err != nil {
		return 0, err
	}
	iv += int64(amount)
	return iv, handle.Set(ctx, key, Int(iv))
}
