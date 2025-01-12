package kvsqlite

import "context"

type TxWithCtx struct {
	tx  Tx
	ctx context.Context
}

func (tx TxWithCtx) Ctx() context.Context {
	return tx.ctx
}
