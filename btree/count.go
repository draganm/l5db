package btree

import "github.com/draganm/l5db/store"

func Count(m store.Memory, a store.Address) (uint64, error) {
	met, err := getMetaNode(m, a)
	if err != nil {
		return 0, err
	}

	return met.count(), nil
}
