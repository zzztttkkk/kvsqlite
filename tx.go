package kvsqlite

import (
	"context"
	"database/sql"
	"fmt"
)

type Tx struct {
	ctx context.Context
	raw *sql.Tx
}

func (tx *Tx) queryone(ctx context.Context, query string, args ...any) *sql.Row {
	// fmt.Println(query, args)
	return tx.raw.QueryRowContext(ctx, query, args...)
}

func (tx *Tx) querymany(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	// fmt.Println(query, args)
	return tx.raw.QueryContext(ctx, query, args...)
}

func (tx *Tx) exec(ctx context.Context, query string, args ...any) error {
	// fmt.Println(query, args)
	_, err := tx.raw.ExecContext(ctx, query, args...)
	return err
}

func (tx *Tx) addkey(ctx context.Context, key string, kind KeyKind) error {
	_, err := tx.raw.ExecContext(ctx, `insert into kv_index (key, kind) values (?, ?)`, key, kind)
	return err
}

func (tx *Tx) ensurekind(ctx context.Context, expected KeyKind, key string) error {
	kind, err := tx.Kind(ctx, key)
	if err != nil {
		return err
	}
	if kind != expected {
		return fmt.Errorf("kvsqlite: bad key kind, expected %s, but it is a %s", expected, kind)
	}
	return nil
}

func (tx *Tx) Kind(ctx context.Context, key string) (KeyKind, error) {
	row := tx.queryone(ctx, `select kind from kv_index where key = ?`, key)
	err := row.Err()
	if err != nil {
		return KeyKind(0), err
	}
	var kind KeyKind
	err = row.Scan(&kind)
	return kind, err
}

func (tx *Tx) Exists(ctx context.Context, key string) (bool, error) {
	_, err := tx.Kind(ctx, key)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (tx *Tx) delone(ctx context.Context, key string) error {
	kind, err := tx.Kind(ctx, key)
	if err != nil {
		return err
	}
	switch kind {
	case KeyKindString:
		{
			return tx.String().delone(ctx, key)
		}
	}
	return nil
}

func (tx *Tx) Del(ctx context.Context, keys ...string) (int, []error) {
	c := 0
	var errors []error
	for _, key := range keys {
		if err := tx.delone(ctx, key); err != nil {
			errors = append(errors, err)
			continue
		}
		c++
	}
	return c, errors
}
