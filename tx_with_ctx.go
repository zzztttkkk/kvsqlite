package kvsqlite

import (
	"context"
	"database/sql"
	"iter"
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

func (handle _HashHandleWithCtx) Incr(filed string, amount int64) (int64, error) {
	return handle._HashHandle.Incr(handle.ctx, filed, amount)
}

type _ListHandleWithCtx struct {
	_ListHandle
	ctx context.Context
}

func (tx *TxWithCtx) List(key string) _ListHandleWithCtx {
	return _ListHandleWithCtx{
		_ListHandle: tx.Tx.List(key),
		ctx:         tx.ctx,
	}
}

func (handle _ListHandleWithCtx) Size() (int, error) {
	return handle._ListHandle.Size(handle.ctx)
}

func (handle _ListHandleWithCtx) Page(page int, pagesize int, order _OrderKind) ([]string, error) {
	return handle._ListHandle.Page(handle.ctx, page, pagesize, order)
}

func (handle _ListHandleWithCtx) AscStream(pagesize int, previdx sql.Null[int]) ([]string, sql.Null[int], error) {
	return handle._ListHandle.AscStream(handle.ctx, pagesize, previdx)
}

func (handle _ListHandleWithCtx) DescStream(pagesize int, previdx sql.Null[int]) ([]string, sql.Null[int], error) {
	return handle._ListHandle.DescStream(handle.ctx, pagesize, previdx)
}

func (handle _ListHandleWithCtx) AscSeq(pagesize int) iter.Seq2[string, error] {
	return handle._ListHandle.AscSeq(handle.ctx, pagesize)
}

func (handle _ListHandleWithCtx) DescSeq(pagesize int) iter.Seq2[string, error] {
	return handle._ListHandle.DescSeq(handle.ctx, pagesize)
}

func (handle _ListHandleWithCtx) GetAll(order _OrderKind) ([]string, error) {
	return handle._ListHandle.GetAll(handle.ctx, order)
}

func (handle _ListHandleWithCtx) First() (string, error) {
	return handle._ListHandle.First(handle.ctx)
}

func (handle _ListHandleWithCtx) Last() (string, error) {
	return handle._ListHandle.Last(handle.ctx)
}

func (handle _ListHandleWithCtx) Nth(idx int) (string, error) {
	return handle._ListHandle.Nth(handle.ctx, idx)
}

func (handle _ListHandleWithCtx) Push(vals ...string) error {
	return handle._ListHandle.Push(handle.ctx, vals...)
}

func (handle _ListHandleWithCtx) LPush(vals ...string) error {
	return handle._ListHandle.LPush(handle.ctx, vals...)
}

func (handle _ListHandleWithCtx) InsertBefore(idx int, vals ...string) error {
	return handle._ListHandle.InsertBefore(handle.ctx, idx, vals...)
}

func (handle _ListHandleWithCtx) InsertAfter(idx int, vals ...string) error {
	return handle._ListHandle.InsertAfter(handle.ctx, idx, vals...)
}

func (handle _ListHandleWithCtx) Clear() error {
	return handle._ListHandle.Clear(handle.ctx)
}

func (handle _ListHandleWithCtx) Remove(idx int, count int) error {
	return handle._ListHandle.Remove(handle.ctx, idx, count)
}

func (handle _ListHandleWithCtx) Pop() (string, error) {
	return handle._ListHandle.Pop(handle.ctx)
}

func (handle _ListHandleWithCtx) LPop() (string, error) {
	return handle._ListHandle.LPop(handle.ctx)
}
