package kvsqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
)

type StringHandle struct {
	tx *Tx
}

func (tx *Tx) String() StringHandle {
	return StringHandle{tx: tx}
}

func (handle StringHandle) ensurekind(ctx context.Context, key string) error {
	return handle.tx.ensurekind(ctx, KeyKindString, key)
}

func (handle StringHandle) Get(ctx context.Context, key string) (string, error) {
	row := handle.tx.queryone(ctx, `select value from kv_string where key = ?`, key)
	err := row.Err()
	if err != nil {
		return "", err
	}
	var val string
	err = row.Scan(&val)
	return val, err
}

func (handle StringHandle) Set(ctx context.Context, key string, val string) error {
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
	return handle.tx.exec(ctx, `insert or replace into kv_string (key, value) values (?, ?)`, key, val)
}

func (handle StringHandle) delone(ctx context.Context, key string) error {
	return handle.tx.exec(ctx, `delete from kv_string where key = ?`, key)
}

func (handle StringHandle) Incr(ctx context.Context, key string, amount int64) (int64, error) {
	pv, err := handle.Get(ctx, key)
	if err != nil {
		if err == sql.ErrNoRows {
			return amount, handle.Set(ctx, key, fmt.Sprintf("%d", amount))
		}
		return 0, err
	}
	iv, err := strconv.ParseInt(pv, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("kvsqlite: incr failed, %s", err)
	}
	iv += int64(amount)
	return iv, handle.Set(ctx, key, fmt.Sprintf("%d", iv))
}
