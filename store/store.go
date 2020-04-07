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
	f, err := os.OpenFile(storeFileName, os.O_CREATE|os.O_RDWR, 0600)
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

func (s *Store) Allocate(size int, t BlockType) (Address, Block, error) {
	nfa := s.nextFreeAddress().UInt64()
	end := nfa + uint64(size+3)
	if end > s.currentSize {
		missing := s.currentSize - end
		toAppend := missing / sizeIncrease

		if (missing % sizeIncrease) > 0 {
			toAppend++
		}

		toAppend *= sizeIncrease

		err := s.f.Truncate(int64(s.currentSize + toAppend))
		if err != nil {
			return 0, nil, errors.Wrapf(err, "while increasing store by %d bytes", toAppend)
		}

		s.currentSize += toAppend
	}

	binary.BigEndian.PutUint64(s.mm, end)

	return Address(nfa), newBlock(s.mm[nfa:end], t), nil

}

func (s *Store) nextFreeAddress() Address {
	return Address(binary.BigEndian.Uint64(s.mm))
}

func (s *Store) GetBlock(addr Address) (Block, error) {
	if addr >= s.nextFreeAddress()-2 {
		return nil, errors.New("block is past the highest address")
	}

	return toBlock(s.mm[addr:])
}

func (s *Store) Free(Address) error {
	return errors.New("not yet implemented")
}

type BlockAllocator interface {
	Allocate(size int) (Address, Block, error)
	Free(Address) error
}
