package kvsqlite

import (
	"context"
	"database/sql"
)

type HashHandle struct {
	tx  *Tx
	ctx context.Context
	key string
}

func (tx *Tx) Hash(ctx context.Context, key string) HashHandle {
	return HashHandle{tx: tx}
}

func (handle HashHandle) Get(ctx context.Context, filed string) (string, error) {
	row := handle.tx.queryone(ctx, `select value from kv_hash where key = ? and field = ?`, handle.key, filed)
	err := row.Err()
	if err != nil {
		return "", err
	}
	var val string
	err = row.Scan(val)
	return val, err
}

func (handle HashHandle) GetAll(ctx context.Context) (map[string]string, error) {
	rows, err := handle.tx.querymany(ctx, `select field, value from kv_hash where key = ?`, handle.key)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var vmap = map[string]string{}
	var key string
	var val string
	for rows.Next() {
		err = rows.Scan(&key, &val)
		if err != nil {
			return nil, err
		}
		vmap[key] = val
	}
	return vmap, nil
}

func (handle HashHandle) Set(ctx context.Context, filed string, val string) error {
	err := handle.tx.ensurekind(ctx, KeyKindHash, handle.key)
	if err != nil {
		if err != sql.ErrNoRows {
			return err
		}
		err = handle.tx.addkey(ctx, handle.key, KeyKindHash)
		if err != nil {
			return err
		}
	}
	return handle.tx.exec(ctx, `insert or replace into kv_hash (key, field, value) values (?, ?, ?)`, handle.key, filed, val)
}
