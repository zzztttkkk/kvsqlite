package kvsqlite

import "github.com/zzztttkkk/lion/enums"

type KeyKind int

const (
	KeyKindUndefined KeyKind = iota
	KeyKindString
	KeyKindHash
	KeyKindList
)

func init() {
	enums.Generate(func() *enums.Options[KeyKind] {
		return &enums.Options[KeyKind]{
			RemoveCommonPrefix: true,
		}
	})
}
