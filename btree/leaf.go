package btree

import (
	"bytes"
	"encoding/binary"

	"github.com/draganm/l5db/store"
	"github.com/pkg/errors"
)

type leaf []byte

// leaf layout:
// 1 byte - number of children
//  2 bytes - key length
//  key bytes
//  8 bytes child address

func createEmptyLeaf(m store.Memory, t byte, keySizeHint uint16) (store.Address, leaf, error) {
	expectedSize := 1 + (2+int(keySizeHint)+8)*(2*int(t)-1)
	ad, bl, err := m.Allocate(expectedSize, store.BTreeLeafBlockType)
	if err != nil {
		return store.NilAddress, nil, errors.Wrap(err, "while allocationg empty btree leaf")
	}

	m.Touch(ad)

	return ad, leaf(bl), nil
}

func (l leaf) put(m store.Memory, myAddress store.Address, key []byte, value store.Address) (store.Address, bool, error) {
	kvs, err := l.kvs()
	if err != nil {
		return store.NilAddress, false, errors.Wrap(err, "while reading key values")
	}

	for _, kv := range kvs {
		if bytes.Equal(kv.key, key) && kv.value == value {
			return store.NilAddress, false, nil
		}
	}

	return store.NilAddress, false, errors.New("TODO: finish the function")

}

type kv struct {
	key   []byte
	value store.Address
}

func (l leaf) keyCount() int {
	return int(l[0])
}

func (l leaf) kvs() ([]kv, error) {
	cnt := l.keyCount()
	kvs := make([]kv, cnt)
	d := l[1:]
	for i := 0; i < cnt; i++ {
		if len(d) < 2 {
			return nil, errors.New("btree leaf malformated: not enough bytes for key length")
		}
		l := int(binary.BigEndian.Uint16(d))
		d = d[2:]

		if len(d) < l {
			return nil, errors.New("btree leaf malformated: not enough bytes for bytes")
		}

		k := d[:l]
		d = d[l:]

		if len(d) < 8 {
			return nil, errors.New("btree leaf malformated: not enough bytes for value address")
		}

		kvs[i].key = k
		kvs[i].value = store.Address(binary.BigEndian.Uint64(d))
		d = d[8:]
	}
	return kvs, nil
}
