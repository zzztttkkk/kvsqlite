// Code generated by "stringer -type KeyKind -trimprefix KeyKind"; DO NOT EDIT.

package kvsqlite

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[KeyKindUndefined-0]
	_ = x[KeyKindString-1]
	_ = x[KeyKindHash-2]
	_ = x[KeyKindList-3]
}

const _KeyKind_name = "UndefinedStringHashList"

var _KeyKind_index = [...]uint8{0, 9, 15, 19, 23}

func (i KeyKind) String() string {
	if i < 0 || i >= KeyKind(len(_KeyKind_index)-1) {
		return "KeyKind(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _KeyKind_name[_KeyKind_index[i]:_KeyKind_index[i+1]]
}
