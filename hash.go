package kvsqlite

import (
	"context"
	"database/sql"
)

type _HashHandle struct {
	tx  *Tx
	key string
}

func (tx *Tx) Hash(key string) _HashHandle {
	return _HashHandle{tx: tx, key: key}
}

func (handle _HashHandle) Get(ctx context.Context, filed string) (Value, error) {
	row := handle.tx.queryone(ctx, `select value from kv_hash where key = ? and field = ?`, handle.key, filed)
	err := row.Err()
	if err != nil {
		return Value{}, err
	}
	var val Value
	err = row.Scan(&val)
	return val, err
}

func (handle _HashHandle) Incr(
	ctx context.Context,
	filed string,
	amount int64,
) (int64, error) {
	prev, err := handle.Get(ctx, filed)
	if err != nil {
		if err == sql.ErrNoRows {
			return amount, handle.Set(ctx, filed, Int(amount))
		} else {
			return 0, err
		}
	}
	num, err := prev.Int64()
	if err != nil {
		return 0, err
	}
	num += amount
	return num, handle.Set(ctx, filed, Int(num))
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

func (handle _HashHandle) Items(ctx context.Context) (map[string]Value, error) {
	rows, err := handle.tx.querymany(ctx, `select field, value from kv_hash where key = ?`, handle.key)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var vmap = map[string]Value{}
	var key string
	var val Value
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

func (handle _HashHandle) Set(ctx context.Context, field string, val Value) error {
	err := handle.ensurekey(ctx)
	if err != nil {
		return err
	}
	_, err = handle.tx.exec(ctx, `insert or replace into kv_hash (key, field, value) values (?, ?, ?)`, handle.key, field, val)
	return err
}

func (handle _HashHandle) SetAll(ctx context.Context, vmap map[string]Value) error {
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

func (handle _HashHandle) Clear(ctx context.Context) (int64, error) {
	err := handle.ensurekey(ctx)
	if err != nil {
		return 0, err
	}
	return handle.tx.exec(ctx, `delete from kv_hash where key=?`, handle.key)
}

func (handle _HashHandle) Del(ctx context.Context, fields ...string) (int64, error) {
	if len(fields) < 1 {
		return 0, nil
	}
	err := handle.ensurekey(ctx)
	if err != nil {
		return 0, err
	}
	stmt, err := handle.tx.stmt(ctx, `delete from kv_hash where key = ? and field = ?`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	var dc int64
	for _, field := range fields {
		result, err := stmt.ExecContext(ctx, handle.key, field)
		if err != nil {
			return 0, err
		}
		ec, err := result.RowsAffected()
		if err != nil {
			return 0, err
		}
		dc += ec
	}
	return dc, nil
}

func (handle _HashHandle) remove(ctx context.Context) (int64, error) {
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
