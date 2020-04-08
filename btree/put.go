package btree

import (
	"github.com/draganm/l5db/store"
	"github.com/pkg/errors"
)

func Put(m store.Memory, a store.Address, key []byte, value store.Address) error {
	b, t, err := m.GetBlock(a)
	if err != nil {
		return errors.Wrap(err, "while getting btree meta block")
	}

	if t != store.BTreeMetaBlockType {
		return errors.Wrap(err, "block is not btree meta block")
	}

	met := meta(b)

	return met.put(m, a, key, value)
}
