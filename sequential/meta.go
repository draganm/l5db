package sequential

import (
	"encoding/binary"

	"github.com/draganm/l5db/store"
	"github.com/pkg/errors"
)

// meta layout:
// 8 bytes - address of the first data block
// 8 bytes - address of the last data block
// 8 bytes - data size
// 2 bytes - data block size

type meta struct {
	m    store.Memory
	addr store.Address
	bl   []byte
}

const metaSize = 26

func createMeta(m store.Memory, blockSize uint16) (store.Address, meta, error) {
	a, d, err := m.Allocate(metaSize, store.SequentialMetaBlockType)
	if err != nil {
		return store.NilAddress, meta{}, errors.Wrap(err, "while allocating sequential meta data")
	}

	binary.BigEndian.PutUint16(d[24:], uint16(blockSize))

	m.Touch(a)

	return a, meta{
		m:    m,
		addr: a,
		bl:   d,
	}, nil
}

func getMeta(m store.Memory, a store.Address) (meta, error) {
	b, t, err := m.GetBlock(a)
	if err != nil {
		return meta{}, errors.Wrap(err, "while getting sequential meta block")
	}

	if t != store.SequentialMetaBlockType {
		return meta{}, errors.Wrap(err, "block is not sequential meta block")
	}

	if len(b) < metaSize {
		return meta{}, errors.Wrapf(err, "store sequential meta block is less than %d bytes", metaSize)
	}

	return meta{
		m:    m,
		addr: a,
		bl:   b,
	}, nil
}

func (m meta) dataSize() uint64 {
	return binary.BigEndian.Uint64(m.bl[16:])
}

func (m meta) append(data []byte) error {
	if m.isEmpty() {
		err := m.createFirstDataBlock()
		if err != nil {
			return errors.Wrap(err, "while creating first data block")
		}
	}

	totalSize := len(data)

	for len(data) > 0 {
		ldb, err := m.getLastDataBlock()
		if err != nil {
			return err
		}

		cnt, err := ldb.append(data)
		if err != nil {
			return err
		}

		data = data[cnt:]
		if len(data) > 0 {
			err = m.appendEmptyBlock()
			if err != nil {
				return err
			}
		}

	}

	return m.addDataSize(uint64(totalSize))

}

func (m meta) appendEmptyBlock() error {
	ldb, err := getData(m.m, m.lastDataBlockAddress())
	if err != nil {
		return err
	}

	ndba, _, err := createEmptyData(m.m, m.blockSize())
	if err != nil {
		return err
	}

	err = ldb.setNextBlockAddress(ndba)
	if err != nil {
		return errors.Wrap(err, "while setting the next block address")
	}

	return m.setLastDataBlock(ndba)
}

func (m meta) isEmpty() bool {
	return m.firstDataBlockAddress() == store.NilAddress
}

func (m meta) addDataSize(n uint64) error {
	binary.BigEndian.PutUint64(m.bl[16:], m.dataSize()+n)
	return m.m.Touch(m.addr)
}

func (m meta) firstDataBlockAddress() store.Address {
	return store.Address(binary.BigEndian.Uint64(m.bl))
}

func (m meta) lastDataBlockAddress() store.Address {
	return store.Address(binary.BigEndian.Uint64(m.bl[8:]))
}

func (m meta) getLastDataBlock() (data, error) {
	return getData(m.m, m.lastDataBlockAddress())
}

func (m meta) getFirstDataBlock() (data, error) {
	return getData(m.m, m.firstDataBlockAddress())
}

func (m meta) firstLastDataBlock() (data, error) {
	return getData(m.m, m.firstDataBlockAddress())
}

func (m meta) blockSize() uint16 {
	return binary.BigEndian.Uint16(m.bl[24:])
}

func (m meta) setFirstDataBlock(a store.Address) error {
	binary.BigEndian.PutUint64(m.bl, a.UInt64())
	return m.m.Touch(m.addr)
}

func (m meta) setLastDataBlock(a store.Address) error {
	binary.BigEndian.PutUint64(m.bl[8:], a.UInt64())
	return m.m.Touch(m.addr)
}

func (m meta) createFirstDataBlock() error {
	a, _, err := createEmptyData(m.m, m.blockSize())
	if err != nil {
		return errors.Wrap(err, "while creating first data block")
	}

	err = m.setFirstDataBlock(a)
	if err != nil {
		return errors.Wrap(err, "while storing first data block")
	}

	err = m.setLastDataBlock(a)
	if err != nil {
		return errors.Wrap(err, "while storing last data block")
	}

	return nil

}

func (m meta) reader() (*reader, error) {
	fdb, err := m.getFirstDataBlock()
	if err != nil {
		return nil, err
	}

	return &reader{
		d: fdb,
	}, nil
}
