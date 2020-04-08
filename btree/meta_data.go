package btree

import (
	"encoding/binary"

	"github.com/draganm/l5db/store"
	"github.com/pkg/errors"
)

type meta []byte

// meta layout:
// 8 bytes - number of keys in the tree
// 8 bytes - Address of the root node / leaf
// 2 bytes - key size hint
// 1 byte - t

func createMeta(m store.Memory, t byte, keySizeHint uint16) (store.Address, meta, error) {
	a, d, err := m.Allocate(11, store.BTreeMetaBlockType)
	if err != nil {
		return store.NilAddress, nil, errors.Wrap(err, "while allocating btree meta data")
	}

	binary.BigEndian.PutUint16(d[8:], uint16(keySizeHint))
	d[10] = byte(t)

	m.Touch(a)

	return a, meta(d), nil
}

func (m meta) count() uint64 {
	return binary.BigEndian.Uint64(m)
}

func (m meta) incrementCount() {
	binary.BigEndian.PutUint64(m, m.count()+1)
}

func (m meta) root() store.Address {
	return store.Address(binary.BigEndian.Uint64(m[:8]))
}

func (m meta) setRoot(r store.Address) {
	binary.BigEndian.PutUint64(m, r.UInt64())
}

func (m meta) put(mem store.Memory, a store.Address, key []byte, value store.Address) error {
	ad := m.root()
	bl, tp, err := mem.GetBlock(ad)
	if err != nil {
		return err
	}

	if tp != store.BTreeLeafBlockType {
		return errors.New("TODO: support more than a leaf")
	}

	l := leaf(bl)

	na, didPut, err := l.put(mem, ad, key, value)
	if err != nil {
		return err
	}

	if didPut {
		m.incrementCount()
	}

	if na != store.NilAddress {
		m.setRoot(na)
	}

	return nil
}
