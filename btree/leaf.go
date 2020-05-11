package btree

import (
	"bytes"
	"encoding/binary"
	"sort"

	"github.com/draganm/l5db/store"
	"github.com/pkg/errors"
)

type leaf struct {
	m           store.Memory
	addr        store.Address
	bl          []byte
	t           byte
	keySizeHint uint16
	kvs         kvs
}

// leaf layout:
// 1 byte - key count
//  2 bytes - key length
//  key bytes
//  8 bytes child address

func createLeaf(m store.Memory, t byte, keySizeHint uint16, kvs kvs) (store.Address, leaf, error) {
	expectedSize := 1 + (2+int(keySizeHint)+8)*(2*int(t))
	ad, bl, err := m.Allocate(expectedSize, store.BTreeLeafBlockType)
	if err != nil {
		return store.NilAddress, leaf{}, errors.Wrap(err, "while allocationg empty btree leaf")
	}

	l := leaf{
		m:           m,
		addr:        ad,
		bl:          bl,
		t:           t,
		keySizeHint: keySizeHint,
		kvs:         kvs,
	}

	err = l.store()
	if err != nil {
		return store.NilAddress, leaf{}, err
	}

	return ad, l, nil
}

func loadLeaf(m store.Memory, a store.Address, t byte, keySizeHint uint16) (leaf, error) {
	bl, tp, err := m.GetBlock(a)
	if err != nil {
		return leaf{}, errors.Wrap(err, "while getting block")
	}

	if tp != store.BTreeLeafBlockType {
		return leaf{}, errors.New("not a btree leaf block")
	}

	cnt := int(bl[0])
	kvs := make(kvs, cnt)
	d := bl[1:]
	for i := 0; i < cnt; i++ {
		if len(d) < 2 {
			return leaf{}, errors.New("btree leaf malformated: not enough bytes for key length")
		}
		l := int(binary.LittleEndian.Uint16(d))
		d = d[2:]

		if len(d) < l {
			return leaf{}, errors.New("btree leaf malformated: not enough bytes for bytes")
		}

		k := d[:l]
		d = d[l:]

		if len(d) < 8 {
			return leaf{}, errors.New("btree leaf malformated: not enough bytes for value address")
		}

		kvs[i].key = copyByteSlice(k)
		kvs[i].value = store.Address(binary.LittleEndian.Uint64(d))
		d = d[8:]
	}

	return leaf{
		m:           m,
		t:           t,
		keySizeHint: keySizeHint,
		addr:        a,
		bl:          bl,
		kvs:         kvs.copy(),
	}, nil

}

func (l leaf) put(key []byte, value store.Address) (store.Address, bool, error) {

	idx := sort.Search(len(l.kvs), func(i int) bool {
		return bytes.Compare(l.kvs[i].key, key) >= 0
	})

	if idx < len(l.kvs) && bytes.Compare(l.kvs[idx].key, key) == 0 {
		kv := l.kvs[idx]
		if bytes.Equal(kv.key, key) && kv.value == value {
			return l.addr, false, nil
		}
		l.kvs[idx].value = value

		err := l.store()
		if err != nil {
			return store.NilAddress, false, errors.Wrap(err, "while storing kvs")
		}

		return l.addr, false, nil
	}

	if l.isFull() {
		return store.NilAddress, false, errors.New("trying to put into full leaf")
	}

	l.kvs = append(l.kvs[:idx], append([]kv{kv{key: key, value: value}}, l.kvs[idx:]...)...)
	err := l.store()
	if err != nil {
		return store.NilAddress, false, errors.Wrap(err, "while storing kvs")
	}

	return l.addr, true, nil

}

func (l leaf) get(key []byte) (store.Address, error) {

	idx := sort.Search(len(l.kvs), func(i int) bool {
		return bytes.Compare(l.kvs[i].key, key) >= 0
	})

	if idx < len(l.kvs) && bytes.Compare(l.kvs[idx].key, key) == 0 {
		return l.kvs[idx].value, nil
	}

	return store.NilAddress, ErrNotFound
}

func (l leaf) keyCount() int {
	return len(l.kvs)
}

func (l leaf) store() error {

	isSorted := sort.SliceIsSorted(l.kvs, func(j, k int) bool {
		return bytes.Compare(l.kvs[j].key, l.kvs[k].key) < 0
	})

	if !isSorted {
		return errors.New("leaf kvs are not sorted")
	}

	for j := 0; j < len(l.kvs)-1; j++ {
		if bytes.Equal(l.kvs[j].key, l.kvs[j+1].key) {
			return errors.New("leaf kvs has duplicate values")
		}
	}

	totalSize := 1

	for _, kv := range l.kvs {
		totalSize += 2 + len(kv.key) + 8
	}

	if totalSize > len(l.bl) {
		return errors.New("TODO: implement allocating larger block")
	}

	d := l.bl

	d[0] = byte(len(l.kvs))

	d = d[1:]

	for _, kv := range l.kvs {
		binary.LittleEndian.PutUint16(d, uint16(len(kv.key)))
		d = d[2:]
		copy(d, kv.key)
		d = d[len(kv.key):]
		binary.LittleEndian.PutUint64(d, kv.value.UInt64())
		d = d[8:]
	}

	l.m.Touch(l.addr)

	return nil

}

func (l leaf) isFull() bool {
	return l.keyCount() == 2*int(l.t)-1
}

func (l leaf) split() (kv, store.Address, store.Address, error) {
	if !l.isFull() {
		return kv{}, store.NilAddress, store.NilAddress, errors.New("trying to split not full node")
	}

	middle := l.kvs[l.t-1].copy()
	left := l.kvs[:l.t-1].copy()
	right := l.kvs[l.t:].copy()

	l.kvs = left.copy()
	err := l.store()
	if err != nil {
		return kv{}, store.NilAddress, store.NilAddress, errors.Wrap(err, "while storing left part of the split child")
	}

	ra, _, err := createLeaf(l.m, l.t, l.keySizeHint, right)
	if err != nil {
		return kv{}, store.NilAddress, store.NilAddress, errors.Wrap(err, "while creating right part of the split child")
	}

	// fmt.Println("leaf left", l.addr, left, "leaf right", ra, right)

	return middle, l.addr, ra, nil

}

func (l leaf) structure() structure {
	return structure{
		Type: "leaf",
		KVS:  l.kvs.copy(),
	}
}
