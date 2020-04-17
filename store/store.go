package store

import (
	"encoding/binary"
	"os"
	"path/filepath"

	"github.com/edsrzf/mmap-go"
	"github.com/pkg/errors"
	"golang.org/x/sys/unix"
)

type Store struct {
	dir         string
	f           *os.File
	mm          mmap.MMap
	currentSize uint64
}

func Open(dir string, maxSize int) (*Store, error) {
	storeFileName := filepath.Join(dir, "db")
	f, err := os.OpenFile(storeFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return nil, errors.Wrapf(err, "while opening file %s", storeFileName)
	}

	st, err := f.Stat()
	if err != nil {
		return nil, errors.Wrapf(err, "while getting stats of file %s", storeFileName)
	}

	currentSize := uint64(st.Size())

	if currentSize == 0 {
		header := make([]byte, 16)
		binary.BigEndian.PutUint64(header, 16)
		_, err = f.Write(header)
		if err != nil {
			return nil, errors.Wrapf(err, "while appending header to %s", storeFileName)
		}
		currentSize = 16
	}

	mm, err := mmap.MapRegion(f, maxSize, mmap.RDWR, 0, 0)

	if err != nil {
		return nil, errors.Wrapf(err, "while memory mapping file %s", storeFileName)
	}

	err = unix.Madvise(mm, unix.MADV_RANDOM)
	if err != nil {
		return nil, errors.Wrapf(err, "while setting madvise to random for segment file %q", storeFileName)
	}

	return &Store{
		dir:         dir,
		f:           f,
		mm:          mm,
		currentSize: currentSize,
	}, nil

}

func (s *Store) Close() error {
	err := s.mm.Unmap()
	if err != nil {
		return errors.Wrapf(err, "while unmmaping %q", s.f.Name())
	}

	err = s.f.Close()
	if err != nil {
		return errors.Wrapf(err, "while closing %s", s.f.Name())
	}

	return nil
}

const sizeIncrease = 16 * 1024 * 1024

func (s *Store) Allocate(size int, t BlockType) (Address, []byte, error) {
	nfa := s.nextFreeAddress().UInt64()
	end := nfa + uint64(size+3)
	if end > s.currentSize {
		missing := end - s.currentSize
		toAppend := missing / sizeIncrease

		if (missing % sizeIncrease) != 0 {
			toAppend++
		}

		toAppend *= sizeIncrease

		err := s.f.Truncate(int64(s.currentSize + toAppend))
		if err != nil {
			return 0, nil, errors.Wrapf(err, "while increasing store by %d bytes", toAppend)
		}

		err = s.f.Sync()
		if err != nil {
			return NilAddress, nil, err
		}

		s.currentSize += toAppend
	}

	// DON'T REMOVE: write new NFA
	binary.BigEndian.PutUint64(s.mm[:8], end)

	binary.BigEndian.PutUint16(s.mm[nfa:nfa+2], uint16(size+3))

	s.mm[nfa+2] = byte(t)

	return Address(nfa + 3), s.mm[nfa+3 : nfa+3+uint64(size)], nil

}

func (s *Store) nextFreeAddress() Address {
	return Address(binary.BigEndian.Uint64(s.mm[:8]))
}

func (s *Store) GetBlock(addr Address) ([]byte, BlockType, error) {

	if addr == NilAddress {
		return nil, 0, errors.New("trying to get block with NIL address")
	}

	nfa := s.nextFreeAddress()
	if addr >= nfa {
		return nil, 0, errors.New("block is past the highest address")
	}

	bld := s.mm[addr-3:]

	l := binary.BigEndian.Uint16(bld)

	bld = bld[:l]
	if len(bld) < 3 {
		return nil, 0, errors.New("block is too short")
	}
	t := BlockType(bld[2])

	return bld[3:], t, nil
}

func (s *Store) Free(Address) error {
	return errors.New("not yet implemented")
}

func (s *Store) Touch(addr Address) error {
	return nil
}

type Memory interface {
	Allocate(size int, t BlockType) (Address, []byte, error)
	Free(Address) error
	GetBlock(addr Address) ([]byte, BlockType, error)
	Touch(Address) error
}

func (s *Store) GetRootAddress() Address {
	return Address(binary.BigEndian.Uint64(s.mm[8:]))
}

func (s *Store) SetRootAddress(a Address) error {
	binary.BigEndian.PutUint64(s.mm[8:], a.UInt64())
	return nil
}
