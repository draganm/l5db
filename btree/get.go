package btree

import (
	serrors "errors"

	"github.com/draganm/l5db/store"
	"github.com/pkg/errors"
)

var ErrNotFound = serrors.New("Key not found")

func Get(m store.Memory, a store.Address, key []byte) (store.Address, error) {
	b, t, err := m.GetBlock(a)
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while getting btree meta block")
	}

	if t != store.BTreeMetaBlockType {
		return store.NilAddress, errors.Wrap(err, "block is not btree meta block")
	}

	met := meta{
		m:    m,
		addr: a,
		bl:   b,
	}

	return met.get(key)

}
