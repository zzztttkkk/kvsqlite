// Code generated by "github.com/zzztttkkk/lion/enums", DO NOT EDIT
// Code generated @ 1736665892

package kvsqlite

import "fmt"



func (ev KeyKind) String() string {
	switch(ev){
		
		case KeyKindUndefined : {
			return "Undefined"
		}
		case KeyKindString : {
			return "String"
		}
		case KeyKindHash : {
			return "Hash"
		}
		case KeyKindList : {
			return "List"
		}
		default: {
			panic(fmt.Errorf("kvsqlite.KeyKind: unknown enum value, %d", ev))
		} 
	}
}





