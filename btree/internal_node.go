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
	kvs         kvs
	children    children
}

// internalNode layout:
// 1 byte - key count
//  (key count+1 * 8 bytes) - children
//  2 bytes - key length
//  key bytes
//  8 bytes child address

func createInternalNode(m store.Memory, t byte, keySizeHint uint16, kvs kvs, children children) (store.Address, internalNode, error) {
	expectedSize := 1 + (2+int(keySizeHint)+8)*(2*int(t)) + 8*(2*int(t)+1)
	ad, bl, err := m.Allocate(expectedSize, store.BTreeInternalNodeBlockType)
	if err != nil {
		return store.NilAddress, internalNode{}, errors.Wrap(err, "while allocationg empty btree internalNode")
	}

	in := internalNode{
		m:           m,
		addr:        ad,
		bl:          bl,
		t:           t,
		keySizeHint: keySizeHint,
		kvs:         kvs,
		children:    children,
	}

	err = in.store()
	if err != nil {
		return store.NilAddress, internalNode{}, err
	}

	return ad, in, nil
}

func copyByteSlice(b []byte) []byte {
	cp := make([]byte, len(b))
	copy(cp, b)
	return cp
}

func loadInternalNode(m store.Memory, a store.Address, t byte, keySizeHint uint16) (internalNode, error) {
	bl, tp, err := m.GetBlock(a)
	if err != nil {
		return internalNode{}, errors.Wrap(err, "while getting block")
	}

	if tp != store.BTreeInternalNodeBlockType {
		return internalNode{}, errors.Wrap(err, "trying to load non- btree internal node as btree internal node")
	}

	cnt := int(bl[0])
	kvs := make(kvs, cnt)
	d := bl[1:]
	for i := 0; i < cnt; i++ {
		if len(d) < 2 {
			return internalNode{}, errors.New("btree internalNode malformated: not enough bytes for key length")
		}
		l := int(binary.LittleEndian.Uint16(d))
		d = d[2:]

		if len(d) < l {
			return internalNode{}, errors.New("btree internalNode malformated: not enough bytes for bytes")
		}

		k := d[:l]
		d = d[l:]

		if len(d) < 8 {
			return internalNode{}, errors.New("btree internalNode malformated: not enough bytes for value address")
		}

		kvs[i].key = copyByteSlice(k)
		kvs[i].value = store.Address(binary.LittleEndian.Uint64(d))
		d = d[8:]
	}

	children := make([]store.Address, cnt+1)

	for i := 0; i < cnt+1; i++ {
		if len(d) < 8 {
			return internalNode{}, errors.New("btree internalNode malformated: not enough bytes for child address")
		}
		children[i] = store.Address(binary.LittleEndian.Uint64(d))
		d = d[8:]
	}

	i := internalNode{
		m:           m,
		addr:        a,
		bl:          bl,
		children:    children,
		keySizeHint: keySizeHint,
		t:           t,
		kvs:         kvs,
	}

	if err != nil {
		return i, err
	}

	return i, nil

}

type localSearchResult struct {
	kvIndex    int
	childIndex int
}

func (lsr localSearchResult) isLocalKV() bool {
	return lsr.kvIndex >= 0
}

func (i internalNode) localSearch(key []byte) localSearchResult {
	idx := sort.Search(len(i.kvs), func(j int) bool {
		return bytes.Compare(i.kvs[j].key, key) >= 0
	})

	if idx < len(i.kvs) && bytes.Compare(i.kvs[idx].key, key) == 0 {
		return localSearchResult{
			kvIndex:    idx,
			childIndex: -1,
		}
	}

	return localSearchResult{
		kvIndex:    -1,
		childIndex: idx,
	}
}

func (i internalNode) put(key []byte, value store.Address) (store.Address, bool, error) {

	lsr := i.localSearch(key)

	if lsr.isLocalKV() {
		i.kvs[lsr.kvIndex].value = value
		err := i.store()
		if err != nil {
			return store.NilAddress, false, err
		}
		return i.addr, false, nil
	}

	childAddress := i.children[lsr.childIndex]

	child, err := getNode(i.m, childAddress, i.t, i.keySizeHint)
	if err != nil {
		return store.NilAddress, false, err
	}

	if child.isFull() {
		if i.isFull() {
			return store.NilAddress, false, errors.Errorf("trying pull up a key/value into a full node %d", i.addr)
		}

		middle, left, right, err := child.split()
		if err != nil {
			return store.NilAddress, false, errors.Wrap(err, "while splitting the child")
		}

		if bytes.Equal(middle.key, key) {
			middle.value = value
		}

		if err != nil {
			return store.NilAddress, false, err
		}

		i.kvs = append(i.kvs[:lsr.childIndex], append([]kv{middle}, i.kvs[lsr.childIndex:]...)...)
		i.children[lsr.childIndex] = left
		i.children = append(i.children[:lsr.childIndex+1], append([]store.Address{right}, i.children[lsr.childIndex+1:]...)...)

		if err != nil {
			return store.NilAddress, false, err
		}

		err = i.store()
		if err != nil {
			return store.NilAddress, false, errors.Wrap(err, "storring split children references")
		}

		if bytes.Equal(middle.key, key) {
			return i.addr, false, nil
		}

		return i.put(key, value)
	}

	nca, inserted, err := child.put(key, value)
	if err != nil {
		return store.NilAddress, false, errors.Wrap(err, "while putting into child")
	}

	if nca != childAddress {
		i.children[lsr.childIndex] = nca
		err = i.store()
		if err != nil {
			return store.NilAddress, false, errors.Wrap(err, "while storing internal node")
		}
	}

	return i.addr, inserted, nil

}

func (i internalNode) get(key []byte) (store.Address, error) {

	lsr := i.localSearch(key)

	if lsr.isLocalKV() {
		return i.kvs[lsr.kvIndex].value, nil
	}

	ch, err := getNode(i.m, i.children[lsr.childIndex], i.t, i.keySizeHint)
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while getting child")
	}

	return ch.get(key)
}

func (i internalNode) keyCount() int {
	return int(i.bl[0])
}

func (i *internalNode) store() error {

	if len(i.kvs) != len(i.children)-1 {
		return errors.Errorf("trying to store %d key/values and %d children", len(i.kvs), len(i.children))
	}

	if len(i.kvs) > (2*int(i.t) - 1) {
		return errors.Errorf("trying to save %d key/values, max %d is allowed", len(i.kvs), (2 * i.t))
	}

	totalSize := 1 + len(i.children)*8

	for _, kv := range i.kvs {
		totalSize += 2 + len(kv.key) + 8
	}

	if totalSize > len(i.bl) {
		return errors.New("TODO: implement allocating larger block")
	}

	d := i.bl

	d[0] = byte(len(i.kvs))

	d = d[1:]

	for _, kv := range i.kvs {
		binary.LittleEndian.PutUint16(d, uint16(len(kv.key)))
		d = d[2:]
		copy(d, kv.key)
		d = d[len(kv.key):]
		binary.LittleEndian.PutUint64(d, kv.value.UInt64())
		d = d[8:]
	}

	for _, c := range i.children {
		binary.LittleEndian.PutUint64(d, c.UInt64())
		d = d[8:]
	}

	i.m.Touch(i.addr)

	return nil

}

func (i internalNode) isFull() bool {
	return len(i.kvs) == 2*int(i.t)-1
}

func (i internalNode) split() (kv, store.Address, store.Address, error) {
	if !i.isFull() {
		return kv{}, store.NilAddress, store.NilAddress, errors.New("trying to split not full node")
	}

	kvs := i.kvs.copy()
	children := i.children.copy()

	middle := kvs[i.t-1]
	left := kvs[:i.t-1]
	leftChildren := children[:i.t]
	rightChildren := children[i.t:]
	right := kvs[i.t:]

	i.kvs = left
	i.children = leftChildren

	err := i.store()
	if err != nil {
		return kv{}, store.NilAddress, store.NilAddress, errors.Wrap(err, "while storing left part of the split child")
	}

	raddr, _, err := createInternalNode(i.m, i.t, i.keySizeHint, right, rightChildren)
	if err != nil {
		return kv{}, store.NilAddress, store.NilAddress, errors.Wrap(err, "while creating right part of the split child")
	}

	return middle, i.addr, raddr, nil

}

func (i internalNode) structure() structure {

	ch := []structure{}

	for _, c := range i.children {
		cn, err := getNode(i.m, c, i.t, i.keySizeHint)
		if err != nil {
			panic(err)
		}
		ch = append(ch, cn.structure())
	}

	return structure{
		Type:     "internal",
		KVS:      i.kvs,
		Children: ch,
	}
}

type children []store.Address

func (c children) copy() children {
	cp := make(children, len(c))
	copy(cp, c)
	return cp
}
