package btree

import (
	"github.com/draganm/l5db/store"
)

func Put(m store.Memory, a store.Address, key []byte, value store.Address) error {
	met, err := getMetaNode(m, a)
	if err != nil {
		return err
	}

	return met.put(key, value)
}
