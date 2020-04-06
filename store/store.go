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
	dir string
	f   *os.File
	mm  mmap.MMap
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

	if st.Size() == 0 {
		header := make([]byte, 16)
		binary.BigEndian.PutUint64(header, 16)
		_, err = f.Write(header)
		if err != nil {
			return nil, errors.Wrapf(err, "while appending header to %s", storeFileName)
		}
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
		dir: dir,
		f:   f,
		mm:  mm,
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
