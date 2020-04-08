package btree

import (
	"github.com/draganm/l5db/store"
)

// type BtreeNode store.Block

func CreateEmptyBTree(a store.Memory, t byte, keySizeHint uint16) (store.Address, error) {

	mda, m, err := createMeta(a, t, keySizeHint)
	if err != nil {
		return store.NilAddress, err
	}

	la, _, err := createEmptyLeaf(a, t, keySizeHint)
	if err != nil {
		return store.NilAddress, err
	}

	m.setRoot(la)

	return mda, nil
}
