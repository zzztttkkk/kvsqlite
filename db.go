package kvsqlite

import (
	"context"
	"database/sql"
	"sync"

	_ "github.com/ncruces/go-sqlite3/driver"
	_ "github.com/ncruces/go-sqlite3/embed"
)

type DB struct {
	lock sync.Mutex
	raw  *sql.DB
}

var (
	lock      sync.Mutex
	instances = map[string]*DB{}
)

func NewDB(fp string) (*DB, error) {
	lock.Lock()
	defer lock.Unlock()

	if fp != ":memory:" {
		pv, ok := instances[fp]
		if ok {
			return pv, nil
		}
	}
	db, err := sql.Open("sqlite3", fp)
	if err != nil {
		return nil, err
	}
	obj := &DB{raw: db}
	instances[fp] = obj
	return obj, nil
}

func (db *DB) Init(ctx context.Context) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	if _, err := db.raw.ExecContext(
		ctx,
		`create table kv_index if not exists (
			key text primary key not null,
			kind int
		)`,
	); err != nil {
		return err
	}

	if _, err := db.raw.ExecContext(
		ctx,
		`create table kv_string if not exists (
			key text primary key not null,
			value text not null
		)`,
	); err != nil {
		return err
	}

	if _, err := db.raw.ExecContext(
		ctx,
		`create table kv_hash if not exists (
			key text primary key not null,
			field text primary key not null,
			value text not null
		)`,
	); err != nil {
		return err
	}

	if _, err := db.raw.ExecContext(
		ctx,
		`create table kv_list if not exists (
			key text primary key not null,
			sort int primary key not null,
			value text not null
		)`,
	); err != nil {
		return err
	}
	return nil
}
