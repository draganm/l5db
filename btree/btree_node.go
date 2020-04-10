package btree

import (
	"github.com/draganm/l5db/store"
	"github.com/pkg/errors"
)

type btreeNode interface {
	put(key []byte, value store.Address) (store.Address, bool, error)
	get(key []byte) (store.Address, error)
}

func getNode(m store.Memory, a store.Address) (btreeNode, error) {
	bl, tp, err := m.GetBlock(a)
	if err != nil {
		return nil, err
	}

	if tp != store.BTreeLeafBlockType {
		return nil, errors.New("TODO: support more than a leaf")
	}

	l := leaf{
		m:    m,
		addr: a,
		bl:   bl,
	}

	return l, nil

}
