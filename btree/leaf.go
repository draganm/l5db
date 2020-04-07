package btree

import (
	"github.com/draganm/l5db/store"
	"github.com/pkg/errors"
)

type leaf []byte

// leaf layout:
// 1 byte - number of children
//  2 bytes - key length
//  key bytes
//  8 bytes child address

func createEmptyLeaf(al store.BlockAllocator, t byte, keySizeHint uint16) (store.Address, leaf, error) {
	expectedSize := (10+int(keySizeHint))*(2*int(t)-1) + 1
	ad, bl, err := al.Allocate(expectedSize, store.BTreeLeafBlockType)
	if err != nil {
		return store.NilAddress, nil, errors.Wrap(err, "while allocationg empty btree leaf")
	}

	return ad, leaf(bl), nil
}
