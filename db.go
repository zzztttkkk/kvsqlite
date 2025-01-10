package kvsqlite

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	_ "github.com/ncruces/go-sqlite3/driver"
	_ "github.com/ncruces/go-sqlite3/embed"
)

type DB struct {
	lock  sync.Mutex
	raw   *sql.DB
	stmts map[string]*sql.Stmt
}

var (
	lock      sync.Mutex
	instances = map[string]*DB{}
)

func OpenDB(ctx context.Context, fp string) (*DB, error) {
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
	obj := &DB{raw: db, stmts: map[string]*sql.Stmt{}}
	err = obj._Init(ctx)
	if err != nil {
		return nil, err
	}
	instances[fp] = obj
	return obj, nil
}

func (db *DB) _Init(ctx context.Context) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	if _, err := db.raw.ExecContext(
		ctx,
		`create table if not exists kv_index (
			key text primary key not null,
			kind int
		)`,
	); err != nil {
		return err
	}

	if _, err := db.raw.ExecContext(
		ctx,
		`create table if not exists kv_string (
			key text primary key not null,
			value text not null
		)`,
	); err != nil {
		return err
	}

	if _, err := db.raw.ExecContext(
		ctx,
		`create table if not exists kv_hash (
			key text not null,
			field text not null,
			value text not null,
			primary key (key, field)
		)`,
	); err != nil {
		return err
	}

	if _, err := db.raw.ExecContext(
		ctx,
		`create table if not exists kv_list (
			key text not null,
			idx int not null,
			value text not null,
			primary key (key, idx)
		);`,
	); err != nil {
		return err
	}
	return nil
}

func (db *DB) Close() error {
	return db.raw.Close()
}

func (db *DB) Scope(ctx context.Context, fnc func(ctx context.Context, tx Tx) error) error {
	sqltx, err := db.raw.Begin()
	if err != nil {
		return err
	}
	defer func() {
		errored := err != nil
		if ra := recover(); ra != nil {
			errored = true
			err = fmt.Errorf("kvsqlite: tx scope recoverd error, %v", ra)
		}
		if errored {
			rollback_err := sqltx.Rollback()
			if rollback_err != nil {
				panic(fmt.Errorf("kvsqlite: %s cause rollback, but rollback failed, %s", err, rollback_err))
			}
			return
		}
		commit_err := sqltx.Commit()
		if commit_err != nil {
			panic(fmt.Errorf("kvsqlite: commit failed, %s", commit_err))
		}
	}()
	err = fnc(ctx, Tx{raw: sqltx, db: db})
	return err
}

func (db *DB) stmt(ctx context.Context, query string) (*sql.Stmt, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	sv, ok := db.stmts[query]
	if ok {
		return sv, nil
	}

	sv, err := db.raw.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	db.stmts[query] = sv
	return sv, nil
}
