package btree

import (
	serrors "errors"

	"github.com/draganm/l5db/store"
)

var ErrNotFound = serrors.New("Key not found")

func Get(m store.Memory, a store.Address, key []byte) (store.Address, error) {
	met, err := getMetaNode(m, a)
	if err != nil {
		return store.NilAddress, err
	}

	return met.get(key)

}
