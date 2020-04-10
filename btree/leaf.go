package btree

import (
	"bytes"
	"encoding/binary"
	"sort"

	"github.com/draganm/l5db/store"
	"github.com/pkg/errors"
)

type leaf struct {
	m    store.Memory
	addr store.Address
	bl   []byte
}

// leaf layout:
// 1 byte - number of children
//  2 bytes - key length
//  key bytes
//  8 bytes child address

func createEmptyLeaf(m store.Memory, t byte, keySizeHint uint16) (store.Address, leaf, error) {
	expectedSize := 1 + (2+int(keySizeHint)+8)*(2*int(t)-1)
	ad, bl, err := m.Allocate(expectedSize, store.BTreeLeafBlockType)
	if err != nil {
		return store.NilAddress, leaf{}, errors.Wrap(err, "while allocationg empty btree leaf")
	}

	m.Touch(ad)

	return ad, leaf{
		m:    m,
		addr: ad,
		bl:   bl,
	}, nil
}

func (l leaf) put(key []byte, value store.Address) (store.Address, bool, error) {
	kvs, err := l.kvs()
	if err != nil {
		return store.NilAddress, false, errors.Wrap(err, "while reading key values")
	}

	idx := sort.Search(len(kvs), func(i int) bool {
		return bytes.Compare(kvs[i].key, key) >= 0
	})

	if idx < len(kvs) && bytes.Compare(kvs[idx].key, key) == 0 {
		kv := kvs[idx]
		if bytes.Equal(kv.key, key) && kv.value == value {
			return l.addr, false, nil
		}
		kvs[idx].value = value

		err = l.storeKVS(kvs)
		if err != nil {
			return store.NilAddress, false, errors.Wrap(err, "while storing kvs")
		}

		return l.addr, false, nil
	}

	kvs = append(kvs[:idx], append([]kv{kv{key: key, value: value}}, kvs[idx:]...)...)
	err = l.storeKVS(kvs)
	if err != nil {
		return store.NilAddress, false, errors.Wrap(err, "while storing kvs")
	}

	return l.addr, true, nil

}

func (l leaf) get(key []byte) (store.Address, error) {
	kvs, err := l.kvs()
	if err != nil {
		return store.NilAddress, err
	}

	idx := sort.Search(len(kvs), func(i int) bool {
		return bytes.Compare(kvs[i].key, key) >= 0
	})

	if idx < len(kvs) && bytes.Compare(kvs[idx].key, key) == 0 {
		return kvs[idx].value, nil
	}

	return store.NilAddress, ErrNotFound
}

type kv struct {
	key   []byte
	value store.Address
}

func (l leaf) keyCount() int {
	return int(l.bl[0])
}

func (l leaf) storeKVS(kvs []kv) error {
	totalSize := 1

	for _, kv := range kvs {
		totalSize += 2 + len(kv.key) + 8
	}

	if totalSize > len(l.bl) {
		return errors.New("TODO: implement allocating larger block")
	}

	d := l.bl

	d[0] = byte(len(kvs))

	d = d[1:]

	for _, kv := range kvs {
		binary.BigEndian.PutUint16(d, uint16(len(kv.key)))
		d = d[2:]
		copy(d, kv.key)
		d = d[len(kv.key):]
		binary.BigEndian.PutUint64(d, kv.value.UInt64())
		d = d[8:]
	}

	l.m.Touch(l.addr)

	return nil

}

func (l leaf) kvs() ([]kv, error) {
	cnt := l.keyCount()
	kvs := make([]kv, cnt)
	d := l.bl[1:]
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
