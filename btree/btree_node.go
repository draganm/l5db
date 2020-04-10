package btree

import "github.com/draganm/l5db/store"

type btreeNode interface {
	put(key []byte, value store.Address) (store.Address, bool, error)
	get(key []byte) (store.Address, error)
}
