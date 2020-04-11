package btree

import (
	"github.com/draganm/l5db/store"
	"github.com/pkg/errors"
)

type btreeNode interface {
	put(key []byte, value store.Address) (store.Address, bool, error)
	get(key []byte) (store.Address, error)
	isFull() bool
	split() (kv, store.Address, store.Address, error)
}

type kv struct {
	key   []byte
	value store.Address
}

func (k kv) copy() kv {
	kc := make([]byte, len(k.key))
	copy(kc, k.key)
	return kv{
		key:   kc,
		value: k.value,
	}
}

type kvs []kv

func (k kvs) copy() kvs {
	c := make(kvs, len(k))
	for i, kv := range k {
		c[i] = kv.copy()
	}
	return c
}

func getNode(m store.Memory, a store.Address, t byte, keySizeHint uint16) (btreeNode, error) {
	bl, tp, err := m.GetBlock(a)
	if err != nil {
		return nil, err
	}

	switch tp {
	case store.BTreeLeafBlockType:
		return leaf{
			m:           m,
			addr:        a,
			bl:          bl,
			t:           t,
			keySizeHint: keySizeHint,
		}, nil
	case store.BTreeInternalNodeBlockType:
		return internalNode{
			m:           m,
			addr:        a,
			bl:          bl,
			t:           t,
			keySizeHint: keySizeHint,
		}, nil
	default:
		return nil, errors.Errorf("unsupported node type %d", tp)

	}

}
