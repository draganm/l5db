package btree

import (
	"encoding/binary"

	"github.com/draganm/l5db/store"
	"github.com/pkg/errors"
)

type meta struct {
	m    store.Memory
	addr store.Address
	bl   []byte
}

// meta layout:
// 8 bytes - number of keys in the tree
// 8 bytes - Address of the root node / leaf
// 2 bytes - key size hint
// 1 byte - t

func createMeta(m store.Memory, t byte, keySizeHint uint16) (store.Address, meta, error) {
	a, d, err := m.Allocate(19, store.BTreeMetaBlockType)
	if err != nil {
		return store.NilAddress, meta{}, errors.Wrap(err, "while allocating btree meta data")
	}

	binary.BigEndian.PutUint16(d[16:], uint16(keySizeHint))
	d[18] = byte(t)

	m.Touch(a)

	return a, meta{
		m:    m,
		addr: a,
		bl:   d,
	}, nil
}

func getMetaNode(m store.Memory, a store.Address) (meta, error) {
	b, t, err := m.GetBlock(a)
	if err != nil {
		return meta{}, errors.Wrap(err, "while getting btree meta block")
	}

	if t != store.BTreeMetaBlockType {
		return meta{}, errors.Wrap(err, "block is not btree meta block")
	}

	return meta{
		m:    m,
		addr: a,
		bl:   b,
	}, nil
}

func (m meta) count() uint64 {
	return binary.BigEndian.Uint64(m.bl)
}

func (m meta) incrementCount() {
	binary.BigEndian.PutUint64(m.bl, m.count()+1)
	m.m.Touch(m.addr)
}

func (m meta) root() store.Address {
	return store.Address(binary.BigEndian.Uint64(m.bl[8:]))
}

func (m meta) setRoot(r store.Address) {
	binary.BigEndian.PutUint64(m.bl[8:], r.UInt64())
	m.m.Touch(m.addr)
}

func (m meta) put(key []byte, value store.Address) error {

	rt, err := m.getRootNode()
	if err != nil {
		return err
	}

	na, didPut, err := rt.put(key, value)
	if err != nil {
		return err
	}

	if didPut {
		m.incrementCount()
	}

	oldRoot := m.root()

	if na != oldRoot {
		m.setRoot(na)
	}

	return nil
}

func (m meta) getRootNode() (btreeNode, error) {
	return getNode(m.m, m.root())
}

func (m meta) get(key []byte) (store.Address, error) {
	rt, err := m.getRootNode()
	if err != nil {
		return store.NilAddress, err
	}

	return rt.get(key)
}
