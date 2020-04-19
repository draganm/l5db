package store

import (
	"github.com/draganm/mmap-go"
	"github.com/pkg/errors"
)

func (s *Store) PrivateMMap() (*Store, error) {

	// use https://godoc.org/github.com/riobard/go-mmap

	// return Open(s.dir, s.maxSize, true)
	// if err != nil {
	// 	return nil, errors.Wrap(err, "wile creating a new fd")
	// }

	mm, err := mmap.MapRegion(s.f, s.maxSize, mmap.RDWR|mmap.COPY|mmap.NORESERVE, 0, 0)

	if err != nil {
		return nil, errors.Wrap(err, "while memory mapping CoW")
	}

	return &Store{
		currentSize: s.currentSize,
		dir:         s.dir,
		f:           s.f,
		maxSize:     s.maxSize,
		mm:          mm,
	}, nil

}
