package kvsqlite

import (
	"context"
	"database/sql"
	"fmt"
)

type Tx struct {
	raw *sql.Tx
}

func (tx *Tx) queryone(ctx context.Context, query string, args ...any) *sql.Row {
	return tx.raw.QueryRowContext(ctx, query, args...)
}

func (tx *Tx) exec(ctx context.Context, query string, args ...any) error {
	_, err := tx.raw.ExecContext(ctx, query, args...)
	return err
}

func (tx *Tx) keykkind(ctx context.Context, key string) (KeyKind, error) {
	row := tx.queryone(ctx, `select kind from kv_index where key = ?`, key)
	err := row.Err()
	if err != nil {
		return KeyKind(0), err
	}
	var kind KeyKind
	err = row.Scan(&kind)
	return kind, err
}

func (tx *Tx) addkey(ctx context.Context, key string, kind KeyKind) error {
	_, err := tx.raw.ExecContext(ctx, `insert into kv_index (key, kind) values (?, ?)`, key, kind)
	return err
}

func (tx *Tx) ensurekind(ctx context.Context, expected KeyKind, key string) error {
	kind, err := tx.keykkind(ctx, key)
	if err != nil {
		return err
	}
	if kind != expected {
		return fmt.Errorf("kvsqlite: bad key kind, expected %s, but it is a %s", expected, kind)
	}
	return nil
}

type StringHandle struct {
	tx *Tx
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
