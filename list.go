package kvsqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"iter"
	"slices"
	"strings"
)

type _ListHandle struct {
	tx  *Tx
	key string
}

func (tx *Tx) List(key string) _ListHandle {
	return _ListHandle{tx: tx, key: key}
}

const KeySetp = 10

func (handle _ListHandle) ensurekey(ctx context.Context) error {
	err := handle.tx.ensurekind(ctx, KeyKindList, handle.key)
	if err != nil {
		if err != sql.ErrNoRows {
			return err
		}
		err = handle.tx.addkey(ctx, handle.key, KeyKindList)
		if err != nil {
			return err
		}
	}
	return err
}

type _OrderKind int

const (
	OrderAsc _OrderKind = iota
	OrderDesc
)

func (kind _OrderKind) String() string {
	switch kind {
	case OrderAsc:
		{
			return "asc"
		}
	case OrderDesc:
		{
			return "desc"
		}
	default:
		{
			panic("")
		}
	}
}

func _OneColScan[T any](rows *sql.Rows, pagesize int) ([]T, error) {
	var lst []T
	if pagesize > 0 {
		lst = make([]T, 0, pagesize)
	}
	var tmp T
	var err error
	for rows.Next() {
		err = rows.Scan(&tmp)
		if err != nil {
			return nil, err
		}
		lst = append(lst, tmp)
	}
	return lst, nil
}

func (handle _ListHandle) Size(ctx context.Context) (int, error) {
	row := handle.tx.queryone(ctx, `select count(idx) from kv_list where key = ?`, handle.key)
	err := row.Err()
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, err
	}
	var lenv int
	err = row.Scan(&lenv)
	return lenv, err
}

func (handle _ListHandle) Page(ctx context.Context, page int, pagesize int, order _OrderKind) ([]string, error) {
	if page < 1 || pagesize < 1 {
		return nil, fmt.Errorf("kvsqlite: bad page/pagesize, %d, %d", page, pagesize)
	}
	var sql string
	var args []any = []any{handle.key}
	{
		order_str := "asc"
		if order == OrderDesc {
			order_str = "desc"
		}
		sql = fmt.Sprintf(`select value from kv_list where key = ? order by idx %s limit %d offset %d`, order_str, pagesize, (page-1)*pagesize)
	}
	rows, err := handle.tx.querymany(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return _OneColScan[string](rows, pagesize)
}

func (handle _ListHandle) _ReadByCursor(ctx context.Context, order _OrderKind, pagesize int, previdx sql.Null[int]) ([]string, sql.Null[int], error) {
	if pagesize < 1 {
		pagesize = 10
	}
	var cmp_op = ">"
	if order == OrderDesc {
		cmp_op = "<"
	}

	var _sql string
	var args []any
	if previdx.Valid {
		_sql = fmt.Sprintf(`select idx, value from kv_list where key = ? and idx %s ? order by idx %s limit ?`, cmp_op, order)
		args = []any{handle.key, previdx.V, pagesize}
	} else {
		_sql = fmt.Sprintf(`select idx, value from kv_list where key = ? order by idx %s limit ?`, order)
		args = []any{handle.key, pagesize}
	}

	rows, err := handle.tx.querymany(ctx, _sql, args...)
	if err != nil {
		return nil, sql.Null[int]{}, err
	}
	defer rows.Close()

	var vals []string
	var storage_idx sql.Null[int]
	for rows.Next() {
		var tmp string
		err = rows.Scan(&storage_idx, &tmp)
		if err != nil {
			return nil, sql.Null[int]{}, err
		}
		vals = append(vals, tmp)
	}
	return vals, storage_idx, nil
}

func (handle _ListHandle) AscStream(ctx context.Context, pagesize int, previdx sql.Null[int]) ([]string, sql.Null[int], error) {
	return handle._ReadByCursor(ctx, OrderAsc, pagesize, previdx)
}

func (handle _ListHandle) DescStream(ctx context.Context, pagesize int, previdx sql.Null[int]) ([]string, sql.Null[int], error) {
	return handle._ReadByCursor(ctx, OrderDesc, pagesize, previdx)
}

func (handle _ListHandle) _Each(ctx context.Context, order _OrderKind, pagesize int) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		var vals []string
		var previdx sql.Null[int]
		var err error

		for {
			vals, previdx, err = handle._ReadByCursor(ctx, order, pagesize, previdx)
			if err != nil {
				yield("", err)
				return
			}
			if len(vals) < 1 {
				break
			}

			for _, v := range vals {
				ok := yield(v, nil)
				if !ok {
					return
				}
			}
		}
	}
}

func (handle _ListHandle) AscSeq(ctx context.Context, pagesize int) iter.Seq2[string, error] {
	return handle._Each(ctx, OrderAsc, pagesize)
}

func (handle _ListHandle) DescSeq(ctx context.Context, pagesize int) iter.Seq2[string, error] {
	return handle._Each(ctx, OrderDesc, pagesize)
}

func (handle _ListHandle) _GetStorageIdxes(ctx context.Context, page int, pagesize int, order _OrderKind) ([]int, error) {
	if page < 1 || pagesize < 1 {
		return nil, fmt.Errorf("kvsqlite: bad page/pagesize, %d, %d", page, pagesize)
	}
	var sql string
	var args []any = []any{handle.key}
	{
		sql = fmt.Sprintf(`select idx from kv_list where key = ? order by idx %s limit %d offset %d`, order, pagesize, (page-1)*pagesize)
	}
	rows, err := handle.tx.querymany(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return _OneColScan[int](rows, pagesize)
}

var (
	ErrBadIdx = errors.New("kvsqlite: bad idx")
)

func (handle _ListHandle) GetAll(ctx context.Context, order _OrderKind) ([]string, error) {
	sql := fmt.Sprintf(`select value from kv_list where key = ? order by idx %s`, order)
	rows, err := handle.tx.querymany(ctx, sql, handle.key)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return _OneColScan[string](rows, 0)
}

func (handle _ListHandle) First(ctx context.Context) (string, error) {
	return handle.Nth(ctx, 0)
}

func (handle _ListHandle) Last(ctx context.Context) (string, error) {
	return handle.Nth(ctx, -1)
}

func (handle _ListHandle) Nth(ctx context.Context, idx int) (string, error) {
	var vals []string
	var err error
	if idx >= 0 {
		vals, err = handle.Page(ctx, idx+1, 1, OrderAsc)
	} else {
		idx = -(idx + 1)
		vals, err = handle.Page(ctx, idx+1, 1, OrderDesc)
	}
	if err != nil {
		return "", err
	}
	if len(vals) < 1 {
		return "", ErrBadIdx
	}
	return vals[0], nil
}

func (handle _ListHandle) _NthStorageIdx(ctx context.Context, idx int) (int, error) {
	var vals []int
	var err error
	if idx >= 0 {
		vals, err = handle._GetStorageIdxes(ctx, idx+1, 1, OrderAsc)
	} else {
		idx = -(idx + 1)
		vals, err = handle._GetStorageIdxes(ctx, idx+1, 1, OrderDesc)
	}
	if err != nil {
		return 0, err
	}
	if len(vals) < 1 {
		return 0, ErrBadIdx
	}
	return vals[0], nil
}

func (handle _ListHandle) insertstmt(ctx context.Context) (*sql.Stmt, error) {
	return handle.tx.stmt(ctx, `insert into kv_list (key, idx, value) values (?, ?, ?)`)
}

func (handle _ListHandle) execinset(ctx context.Context, stmt *sql.Stmt, storageidx int, value string) error {
	_, err := stmt.ExecContext(ctx, handle.key, storageidx, value)
	return err
}

func (handle _ListHandle) reidxstmt(ctx context.Context) (*sql.Stmt, error) {
	return handle.tx.stmt(ctx, `update kv_list set idx = ? where key = ? and idx = ?`)
}

func (handle _ListHandle) execreidx(ctx context.Context, stmt *sql.Stmt, prev, current int) error {
	_, err := stmt.ExecContext(ctx, current, handle.key, prev)
	return err
}

func (handle _ListHandle) _DoPush(ctx context.Context, idx int, step int, vals ...string) error {
	err := handle.ensurekey(ctx)
	if err != nil {
		return err
	}
	storage_idx, err := handle._NthStorageIdx(ctx, idx)
	if err != nil {
		if err == ErrBadIdx {
			storage_idx = 0
		} else {
			return err
		}
	}
	stmt, err := handle.insertstmt(ctx)
	if err != nil {
		return err
	}
	for _, val := range vals {
		storage_idx += step
		err = handle.execinset(ctx, stmt, storage_idx, val)
		if err != nil {
			return err
		}
	}
	return nil
}

func (handle _ListHandle) Push(ctx context.Context, vals ...string) error {
	return handle._DoPush(ctx, -1, KeySetp, vals...)
}

func (handle _ListHandle) LPush(ctx context.Context, vals ...string) error {
	return handle._DoPush(ctx, 0, -KeySetp, vals...)
}

func (handle _ListHandle) fixidx(ctx context.Context, idx int) (int, int, error) {
	_raw_idx := idx
	err := handle.ensurekey(ctx)
	if err != nil {
		return 0, 0, err
	}

	size, err := handle.Size(ctx)
	if err != nil {
		return 0, 0, err
	}

	if idx < 0 {
		idx = size + idx
		if idx < 0 {
			return 0, 0, fmt.Errorf("kvsqlite: bad idx, %d, current size: %d", _raw_idx, size)
		}
	}
	if idx >= size {
		return 0, 0, fmt.Errorf("kvsqlite: bad idx, %d, current size: %d", _raw_idx, size)
	}
	return size, idx, nil
}

func (handle _ListHandle) InsertBefore(ctx context.Context, idx int, vals ...string) error {
	size, idx, err := handle.fixidx(ctx, idx)
	if err != nil {
		return err
	}
	if idx == 0 {
		slices.Reverse(vals)
		return handle.LPush(ctx, vals...)
	}
	bsi, err := handle.reidx(ctx, size, idx-1, idx, len(vals))
	if err != nil {
		return err
	}
	stmt, err := handle.insertstmt(ctx)
	if err != nil {
		return err
	}
	for _, val := range vals {
		err = handle.execinset(ctx, stmt, bsi, val)
		if err != nil {
			return err
		}
		bsi += KeySetp
	}
	return nil
}

func (handle _ListHandle) reidx(ctx context.Context, size int, lidx int, ridx int, count int) (int, error) {
	if ridx-lidx != 1 {
		return 0, fmt.Errorf("kvsqlite: list.reidx failed, bad idxes, l: %d, r: %d", lidx, ridx)
	}
	var rows *sql.Rows
	var storage_idx_begin int
	var insert_storage_idx_begin int
	if lidx < size/2 {
		r_storage_idx, err := handle._NthStorageIdx(ctx, ridx)
		if err != nil {
			return 0, err
		}
		rows, err = handle.tx.querymany(ctx, `select idx from kv_list where key = ? order by idx asc limit ?`, handle.key, ridx)
		if err != nil {
			return 0, err
		}
		storage_idx_begin = r_storage_idx - (count+ridx)*KeySetp
		insert_storage_idx_begin = r_storage_idx - count*KeySetp
	} else {
		l_storage_idx, err := handle._NthStorageIdx(ctx, lidx)
		if err != nil {
			return 0, err
		}
		rows, err = handle.tx.querymany(ctx, `select idx from kv_list where key = ? order by idx desc limit ?`, handle.key, size-lidx-1)
		if err != nil {
			return 0, err
		}
		storage_idx_begin = l_storage_idx + (count+1)*KeySetp
		insert_storage_idx_begin = l_storage_idx + KeySetp
	}
	defer rows.Close()

	stmt, err := handle.reidxstmt(ctx)
	if err != nil {
		return 0, err
	}

	type Item struct {
		PrevStorageIdx int
		NewStorageIdx  int
	}

	var tmps []Item

	consume_tmps := func() error {
		if len(tmps) < 1 {
			return nil
		}
		for _, tmp := range tmps {
			if e := handle.execreidx(ctx, stmt, tmp.PrevStorageIdx, tmp.NewStorageIdx); e != nil {
				return e
			}
		}
		return nil
	}

	for rows.Next() {
		var sitmp int
		err = rows.Scan(&sitmp)
		if err != nil {
			return 0, err
		}
		tmps = append(tmps, Item{PrevStorageIdx: sitmp, NewStorageIdx: storage_idx_begin})
		storage_idx_begin += 10

		if len(tmps) >= 10 {
			err = consume_tmps()
			if err != nil {
				return 0, err
			}
		}
	}
	return insert_storage_idx_begin, consume_tmps()
}

func (handle _ListHandle) InsertAfter(ctx context.Context, idx int, vals ...string) error {
	size, idx, err := handle.fixidx(ctx, idx)
	if err != nil {
		return err
	}
	if idx == size-1 {
		return handle.Push(ctx, vals...)
	}
	bsi, err := handle.reidx(ctx, size, idx, idx+1, len(vals))
	if err != nil {
		return err
	}
	stmt, err := handle.insertstmt(ctx)
	if err != nil {
		return err
	}
	for _, val := range vals {
		err = handle.execinset(ctx, stmt, bsi, val)
		if err != nil {
			return err
		}
		bsi += KeySetp
	}
	return nil
}

func (handle _ListHandle) remove(ctx context.Context) error {
	return handle.tx.exec(ctx, `delete from kv_list where key = ?`, handle.key)
}

func (handle _ListHandle) Clear(ctx context.Context) error {
	return handle.remove(ctx)
}

func (handle _ListHandle) Remove(ctx context.Context, idx int, count int) error {
	if count < 1 {
		return nil
	}
	storage_idx, err := handle._NthStorageIdx(ctx, idx)
	if err != nil {
		return err
	}

	// https://github.com/ncruces/go-sqlite3/issues/213
	// TODO mkae a custom sqlite.wasm
	// return handle.tx.exec(ctx, `delete from kv_list where key = ? and idx >= ? limit ?`, handle.key, storage_idx, count)

	rows, err := handle.tx.querymany(ctx, `select idx from kv_list where key = ? and  idx >= ? limit ?`, handle.key, storage_idx, count)
	if err != nil {
		return err
	}
	defer rows.Close()

	var idxes = make([]int, 0, count)
	for rows.Next() {
		var idx int
		err = rows.Scan(&idx)
		if err != nil {
			return err
		}
		idxes = append(idxes, idx)
	}
	if len(idxes) < 1 {
		return sql.ErrNoRows
	}
	var sb strings.Builder
	sb.WriteByte('(')
	for i, idx := range idxes {
		sb.WriteString(fmt.Sprintf("%d", idx))
		if i < len(idxes)-1 {
			sb.WriteByte(',')
		}
	}
	sb.WriteByte(')')
	return handle.tx.exec(ctx, fmt.Sprintf(`delete from kv_list where key = ? and idx in %s`, sb.String()), handle.key)
}

func (handle _ListHandle) _Pop(ctx context.Context, order _OrderKind) (string, error) {
	row := handle.tx.queryone(
		ctx,
		fmt.Sprintf(`select idx, value from kv_list where key = ? order by idx %s limit 1`, order),
		handle.key,
	)
	err := row.Err()
	if err != nil {
		return "", err
	}
	var idx int
	var val string
	err = row.Scan(&idx, &val)
	if err != nil {
		return "", err
	}
	err = handle.tx.exec(
		ctx,
		`delete from kv_list where key = ? and idx = ?`,
		handle.key, idx,
	)
	return val, err
}

func (handle _ListHandle) Pop(ctx context.Context) (string, error) {
	return handle._Pop(ctx, OrderDesc)
}

func (handle _ListHandle) LPop(ctx context.Context) (string, error) {
	return handle._Pop(ctx, OrderAsc)
}
