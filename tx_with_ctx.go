package kvsqlite

import (
	"context"
)

type TxWithCtx struct {
	Tx
	ctx context.Context
}

func (tx *TxWithCtx) Kind(key string) (KeyKind, error) {
	return tx.Tx.Kind(tx.ctx, key)
}

func (tx *TxWithCtx) Exists(key string) (bool, error) {
	return tx.Tx.Exists(tx.ctx, key)
}

func (tx *TxWithCtx) Del(keys ...string) (int, []error) {
	return tx.Tx.Del(tx.ctx, keys...)
}

type _StringHandleWithCtx struct {
	_StringHandle
	ctx context.Context
}

func (tx *TxWithCtx) String() _StringHandleWithCtx {
	return _StringHandleWithCtx{
		_StringHandle: tx.Tx.String(),
		ctx:           tx.ctx,
	}
}

func (handle _StringHandleWithCtx) Get(key string) (string, error) {
	return handle._StringHandle.Get(handle.ctx, key)
}

func (handle _StringHandleWithCtx) Set(key string, val string) error {
	return handle._StringHandle.Set(handle.ctx, key, val)
}

func (handle _StringHandleWithCtx) Incr(key string, amount int64) (int64, error) {
	return handle._StringHandle.Incr(handle.ctx, key, amount)
}

type _HashHandleWithCtx struct {
	_HashHandle
	ctx context.Context
}

func (tx *TxWithCtx) Hash(key string) _HashHandleWithCtx {
	return _HashHandleWithCtx{
		_HashHandle: tx.Tx.Hash(key),
		ctx:         tx.ctx,
	}
}

func (handle _HashHandleWithCtx) Get(filed string) (string, error) {
	return handle._HashHandle.Get(handle.ctx, filed)
}

func (handle _HashHandleWithCtx) Exists(filed string) (bool, error) {
	return handle._HashHandle.Exists(handle.ctx, filed)
}

func (handle _HashHandleWithCtx) Size() (int, error) {
	return handle._HashHandle.Size(handle.ctx)
}

func (handle _HashHandleWithCtx) GetAll() (map[string]string, error) {
	return handle._HashHandle.GetAll(handle.ctx)
}

func (handle _HashHandleWithCtx) Set(filed string, val string) error {
	return handle._HashHandle.Set(handle.ctx, filed, val)
}

func (handle _HashHandleWithCtx) SetAll(vmap map[string]string) error {
	return handle._HashHandle.SetAll(handle.ctx, vmap)
}

func (handle _HashHandleWithCtx) Clear() error {
	return handle._HashHandle.Clear(handle.ctx)
}

func (handle _HashHandleWithCtx) Del(fields ...string) error {
	return handle._HashHandle.Del(handle.ctx, fields...)
}

func (handle _HashHandleWithCtx) Keys() ([]string, error) {
	return handle._HashHandle.Keys(handle.ctx)
}
