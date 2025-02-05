package kvsqlite

//go:generate stringer -type KeyKind -trimprefix "KeyKind"
type KeyKind int

const (
	KeyKindUndefined KeyKind = iota
	KeyKindString
	KeyKindHash
	KeyKindList
)
