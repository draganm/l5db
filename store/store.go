package store

import (
	"encoding/binary"
	"os"
	"path/filepath"

	"github.com/draganm/mmap-go"
	"github.com/pkg/errors"
	"golang.org/x/sys/unix"
)

type Store struct {
	dir         string
	f           *os.File
	mm          mmap.MMap
	currentSize uint64
	maxSize     int
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

	mmFlags := mmap.RDWR

	mm, err := mmap.MapRegion(f, maxSize, mmFlags, 0, 0)

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
		maxSize:     maxSize,
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

func bitsForSize(size int) int {
	var bits = 3

	for ; size>>bits > 0; bits++ {
	}

	return bits

}

func (s *Store) Allocate(size int, t BlockType) (Address, []byte, error) {

	bits := bitsForSize(size + 2)
	bitsSize := 1 << bits

	nfa := s.nextFreeAddress().UInt64()
	end := nfa + uint64(bitsSize)
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

	s.mm[nfa] = byte(bits)
	s.mm[nfa+1] = byte(t)

	return Address(nfa + 2), s.mm[nfa+2 : nfa+2+uint64(size)], nil

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

	bld := s.mm[addr-2:]
	bits := bld[0]

	l := 1 << int(bits)

	bld = bld[:l]
	if len(bld) < 2 {
		return nil, 0, errors.New("block is too short")
	}
	t := BlockType(bld[1])

	return bld[2:], t, nil
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
