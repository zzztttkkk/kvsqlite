package kvsqlite

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strconv"
	"unsafe"
)

type Value struct {
	Bytes []byte
}

func b2s(bs []byte) string {
	return unsafe.String(unsafe.SliceData(bs), len(bs))
}

func s2b(sv string) []byte {
	return unsafe.Slice(unsafe.StringData(sv), len(sv))
}

func Int(v int64) Value {
	return String(strconv.FormatInt(v, 10))
}

func Bool(v bool) Value {
	return String(strconv.FormatBool(v))
}

func Bytes(v []byte) Value {
	return Value{
		Bytes: v,
	}
}

func JSON(v any) (Value, error) {
	bs, err := json.Marshal(v)
	if err != nil {
		return Value{}, err
	}
	return Value{Bytes: bs}, nil
}

func JSONIdent(v any) (Value, error) {
	bs, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return Value{}, err
	}
	return Value{Bytes: bs}, nil
}

func MustJSON(v any) Value {
	val, err := JSON(v)
	if err != nil {
		panic(err)
	}
	return val
}

func MustJSONIdent(v any) Value {
	val, err := JSONIdent(v)
	if err != nil {
		panic(err)
	}
	return val
}

func String(v string) Value {
	return Value{
		Bytes: s2b(v),
	}
}

func (v *Value) Scan(src any) error {
	switch tv := src.(type) {
	case []byte:
		{
			v.Bytes = append(v.Bytes, tv...)
			return nil
		}
	default:
		{
			return fmt.Errorf("kvsqlite: bad value column type")
		}
	}
}

func (v Value) Value() (driver.Value, error) {
	return v.Bytes, nil
}

func (v *Value) String() string {
	return b2s(v.Bytes)
}

func (v *Value) Int64() (int64, error) {
	return strconv.ParseInt(v.String(), 10, 64)
}

func (v *Value) Bool() (bool, error) {
	return strconv.ParseBool(v.String())
}

func (v *Value) UnmarshalJSON(dst any) error {
	return json.Unmarshal(v.Bytes, dst)
}
