package btree

import (
	"bytes"
	"encoding/binary"
	"sort"

	"github.com/draganm/l5db/store"
	"github.com/pkg/errors"
)

type internalNode struct {
	m           store.Memory
	addr        store.Address
	bl          []byte
	t           byte
	keySizeHint uint16
}

// internalNode layout:
// 1 byte - key count
//  (key count+1 * 8 bytes) - children
//  2 bytes - key length
//  key bytes
//  8 bytes child address

func createEmptyInternalNode(m store.Memory, t byte, keySizeHint uint16) (store.Address, internalNode, error) {
	expectedSize := 1 + (2+int(keySizeHint)+8)*(2*int(t)-1) + 8*(2*int(t)-1+1)
	ad, bl, err := m.Allocate(expectedSize, store.BTreeInternalNodeBlockType)
	if err != nil {
		return store.NilAddress, internalNode{}, errors.Wrap(err, "while allocationg empty btree internalNode")
	}

	m.Touch(ad)

	return ad, internalNode{
		m:           m,
		addr:        ad,
		bl:          bl,
		t:           t,
		keySizeHint: keySizeHint,
	}, nil
}

func (i internalNode) put(key []byte, value store.Address) (store.Address, bool, error) {
	kvs, children, err := i.kvsAndChildren()
	if err != nil {
		return store.NilAddress, false, errors.Wrap(err, "while reading key values")
	}

	idx := sort.Search(len(kvs), func(i int) bool {
		return bytes.Compare(kvs[i].key, key) >= 0
	})

	if idx < len(kvs) && bytes.Compare(kvs[idx].key, key) == 0 {
		kv := kvs[idx]
		if bytes.Equal(kv.key, key) && kv.value == value {
			return i.addr, false, nil
		}
		kvs[idx].value = value

		err = i.storeKVSAndChildren(kvs, children)
		if err != nil {
			return store.NilAddress, false, errors.Wrap(err, "while storing kvs")
		}

		return i.addr, false, nil
	}

	childAddress := children[idx]

	child, err := getNode(i.m, childAddress, i.t, i.keySizeHint)

	if child.isFull() {
		// split the child, re-try the put
		middle, left, right, err := child.split()
		if err != nil {
			return store.NilAddress, false, errors.Wrap(err, "while splitting the child")
		}

		kvs = append(kvs[:idx], append([]kv{middle}, kvs[idx:]...)...)
		children[idx] = left
		children = append(children[:idx], append([]store.Address{left, right}, children[idx:]...)...)

		err = i.storeKVSAndChildren(kvs, children)
		if err != nil {
			return store.NilAddress, false, errors.Wrap(err, "storring split children references")
		}

		return i.put(key, value)
	}

	nca, inserted, err := child.put(key, value)
	if err != nil {
		return store.NilAddress, false, errors.Wrap(err, "while putting into child")
	}

	if nca != childAddress {
		children[idx] = nca
		err = i.storeKVSAndChildren(kvs, children)
		if err != nil {
			return store.NilAddress, false, errors.Wrap(err, "while storing internal node")
		}
	}

	return i.addr, inserted, nil

}

func (i internalNode) get(key []byte) (store.Address, error) {
	kvs, children, err := i.kvsAndChildren()
	if err != nil {
		return store.NilAddress, err
	}

	idx := sort.Search(len(kvs), func(i int) bool {
		return bytes.Compare(kvs[i].key, key) >= 0
	})

	if idx < len(kvs) && bytes.Compare(kvs[idx].key, key) == 0 {
		return kvs[idx].value, nil
	}

	ch, err := getNode(i.m, children[idx], i.t, i.keySizeHint)
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while getting child")
	}

	return ch.get(key)

}

func (i internalNode) keyCount() int {
	return int(i.bl[0])
}

func (i internalNode) storeKVSAndChildren(kvs []kv, children []store.Address) error {
	totalSize := 1

	for _, kv := range kvs {
		totalSize += 2 + len(kv.key) + 8
	}

	if totalSize > len(i.bl) {
		return errors.New("TODO: implement allocating larger block")
	}

	d := i.bl

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

	for _, c := range children {
		binary.BigEndian.PutUint64(d, c.UInt64())
		d = d[8:]
	}

	i.m.Touch(i.addr)

	return nil

}

func (i internalNode) kvsAndChildren() (kvs, []store.Address, error) {
	cnt := i.keyCount()
	kvs := make([]kv, cnt)
	d := i.bl[1:]
	for i := 0; i < cnt; i++ {
		if len(d) < 2 {
			return nil, nil, errors.New("btree internalNode malformated: not enough bytes for key length")
		}
		l := int(binary.BigEndian.Uint16(d))
		d = d[2:]

		if len(d) < l {
			return nil, nil, errors.New("btree internalNode malformated: not enough bytes for bytes")
		}

		k := d[:l]
		d = d[l:]

		if len(d) < 8 {
			return nil, nil, errors.New("btree internalNode malformated: not enough bytes for value address")
		}

		kvs[i].key = k
		kvs[i].value = store.Address(binary.BigEndian.Uint64(d))
		d = d[8:]
	}

	children := make([]store.Address, cnt+1)

	for i := 0; i < cnt+1; i++ {
		if len(d) < 8 {
			return nil, nil, errors.New("btree internalNode malformated: not enough bytes for child address")
		}
		children[i] = store.Address(binary.BigEndian.Uint64(d))
		d = d[8:]
	}

	return kvs, children, nil
}

func (i internalNode) isFull() bool {
	return i.keyCount() == 2*int(i.t)-2
}

func (i internalNode) split() (kv, store.Address, store.Address, error) {
	if !i.isFull() {
		return kv{}, store.NilAddress, store.NilAddress, errors.New("trying to split not full node")
	}

	kvs, children, err := i.kvsAndChildren()

	if err != nil {
		return kv{}, store.NilAddress, store.NilAddress, err
	}

	middle := kvs[i.t-1].copy()
	left := kvs[:i.t-1].copy()
	leftChildren := children[:i.t]
	rightChildren := children[i.t:]
	right := kvs[i.t:].copy()

	err = i.storeKVSAndChildren(left, leftChildren)
	if err != nil {
		return kv{}, store.NilAddress, store.NilAddress, errors.Wrap(err, "while storing left part of the split child")
	}

	_, ri, err := createEmptyInternalNode(i.m, i.t, i.keySizeHint)
	if err != nil {
		return kv{}, store.NilAddress, store.NilAddress, errors.Wrap(err, "while creating right part of the split child")
	}

	err = ri.storeKVSAndChildren(right, rightChildren)
	if err != nil {
		return kv{}, store.NilAddress, store.NilAddress, errors.Wrap(err, "while storing right part of the split child")
	}

	return middle, i.addr, ri.addr, nil

}

func (i internalNode) structure() structure {
	kvs, children, err := i.kvsAndChildren()
	if err != nil {
		panic(err)
	}

	ch := []structure{}

	for _, c := range children {
		cn, err := getNode(i.m, c, i.t, i.keySizeHint)
		if err != nil {
			panic(err)
		}
		ch = append(ch, cn.structure())
	}

	return structure{
		Type:     "internal",
		KVS:      kvs,
		Children: ch,
	}
}
