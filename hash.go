package kvsqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
)

type _HashHandle struct {
	tx  *Tx
	key string
}

func (tx *Tx) Hash(key string) _HashHandle {
	return _HashHandle{tx: tx, key: key}
}

func (handle _HashHandle) Get(ctx context.Context, filed string) (string, error) {
	row := handle.tx.queryone(ctx, `select value from kv_hash where key = ? and field = ?`, handle.key, filed)
	err := row.Err()
	if err != nil {
		return "", err
	}
	var val string
	err = row.Scan(&val)
	return val, err
}

func (handle _HashHandle) Incr(cxt context.Context, filed string, amount int64) (int64, error) {
	prevs, err := handle.Get(cxt, filed)
	if err != nil {
		if err == sql.ErrNoRows {
			return amount, handle.Set(cxt, filed, fmt.Sprintf("%d", amount))
		} else {
			return 0, err
		}
	}
	num, err := strconv.ParseInt(prevs, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("kvsqlite: prev value is  not a int, %s", prevs)
	}
	num += amount
	return num, handle.Set(cxt, filed, fmt.Sprintf("%d", num))
}

func (handle _HashHandle) Exists(ctx context.Context, filed string) (bool, error) {
	_, err := handle.Get(ctx, filed)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (handle _HashHandle) Size(ctx context.Context) (int, error) {
	row := handle.tx.queryone(ctx, `select count(field) from kv_hash where key = ?`, handle.key)
	err := row.Err()
	if err != nil {
		return 0, err
	}
	var c int
	err = row.Scan(&c)
	return c, err
}

func (handle _HashHandle) GetAll(ctx context.Context) (map[string]string, error) {
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

func (handle _HashHandle) ensurekey(ctx context.Context) error {
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
	return err
}

func (handle _HashHandle) Set(ctx context.Context, filed string, val string) error {
	err := handle.ensurekey(ctx)
	if err != nil {
		return err
	}
	return handle.tx.exec(ctx, `insert or replace into kv_hash (key, field, value) values (?, ?, ?)`, handle.key, filed, val)
}

func (handle _HashHandle) SetAll(ctx context.Context, vmap map[string]string) error {
	if len(vmap) < 1 {
		return nil
	}
	err := handle.ensurekey(ctx)
	if err != nil {
		return err
	}

	stmt, err := handle.tx.stmt(ctx, `insert or replace into kv_hash (key, field, value) values (?, ?, ?)`)
	if err != nil {
		return err
	}
	for field, val := range vmap {
		_, err = stmt.ExecContext(ctx, handle.key, field, val)
		if err != nil {
			return err
		}
	}
	return nil
}

func (handle _HashHandle) Clear(ctx context.Context) error {
	err := handle.ensurekey(ctx)
	if err != nil {
		return err
	}
	return handle.tx.exec(ctx, `delete from kv_hash where key=?`, handle.key)
}

func (handle _HashHandle) Del(ctx context.Context, fields ...string) error {
	if len(fields) < 1 {
		return nil
	}
	err := handle.ensurekey(ctx)
	if err != nil {
		return err
	}
	stmt, err := handle.tx.stmt(ctx, `delete from kv_hash where key = ? and field = ?`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, field := range fields {
		_, err = stmt.ExecContext(ctx, handle.key, field)
		if err != nil {
			return err
		}
	}
	return nil
}

func (handle _HashHandle) remove(ctx context.Context) error {
	return handle.tx.exec(ctx, `delete from kv_hash where key = ?`, handle.key)
}

func (handle _HashHandle) Keys(ctx context.Context) ([]string, error) {
	rows, err := handle.tx.querymany(ctx, `select field from kv_hash where key = ?`, handle.key)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var keys []string
	var tmp string
	for rows.Next() {
		err = rows.Scan(&tmp)
		if err != nil {
			return nil, err
		}
		keys = append(keys, tmp)
	}
	return keys, nil
}
