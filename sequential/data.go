package sequential

import (
	"encoding/binary"

	"github.com/draganm/l5db/store"
	"github.com/pkg/errors"
)

// format
// 8 bytes - next block
// 2 bytes - data size
// data size bytes: data

const dataHeaderSize = 10

type data struct {
	m    store.Memory
	addr store.Address
	bl   []byte
}

func createEmptyData(m store.Memory, blockSize uint16) (store.Address, data, error) {
	addr, d, err := m.Allocate(int(blockSize)+dataHeaderSize, store.SequentialDataBlockType)
	if err != nil {
		return store.NilAddress, data{}, errors.Wrap(err, "while allocating block data")
	}

	return addr, data{
		m:    m,
		addr: addr,
		bl:   d,
	}, nil

}

func getData(m store.Memory, a store.Address) (data, error) {
	bl, bt, err := m.GetBlock(a)
	if err != nil {
		return data{}, err
	}

	if bt != store.SequentialDataBlockType {
		return data{}, errors.New("not a sequential data block type")
	}

	return data{
		m:    m,
		addr: a,
		bl:   bl,
	}, nil

}

func (d data) append(bytes []byte) (int, error) {
	remaining, fromIndex := d.remainingCapacity()

	if remaining > 0 {

		toDo := len(bytes)
		if remaining < toDo {
			toDo = remaining
		}

		copy(d.bl[fromIndex:], bytes)
		err := d.m.Touch(d.addr)
		if err != nil {
			return 0, err
		}
		return toDo, d.increaseDataSize(uint16(toDo))
	}

	return 0, nil
}

func (d data) payload() []byte {
	return d.bl[dataHeaderSize : dataHeaderSize+d.dataSize()]
}

func (d data) dataSize() uint16 {
	return binary.BigEndian.Uint16(d.bl[8:])
}

func (d data) increaseDataSize(delta uint16) error {
	nds := d.dataSize() + delta
	binary.BigEndian.PutUint16(d.bl[8:], nds)
	return d.m.Touch(d.addr)
}

func (d data) remainingCapacity() (int, int) {
	stored := int(binary.BigEndian.Uint16(d.bl[8:]))
	total := len(d.bl) - dataHeaderSize

	return total - stored, dataHeaderSize + stored
}

func (d data) nextBlockAddress() store.Address {
	return store.Address(binary.BigEndian.Uint64(d.bl))
}

func (d data) setNextBlockAddress(a store.Address) error {
	binary.BigEndian.PutUint64(d.bl, a.UInt64())
	return d.m.Touch(d.addr)
}

func (d data) hasNextBlock() bool {
	return d.nextBlockAddress() != store.NilAddress
}

func (d data) nextBlock() (data, error) {
	nba := d.nextBlockAddress()
	if nba == store.NilAddress {
		return data{}, errors.New("this was the last block")
	}

	return getData(d.m, nba)
}
