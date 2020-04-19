package l5db

import (
	"sync"

	"github.com/draganm/l5db/btree"
	"github.com/draganm/l5db/store"
	"github.com/pkg/errors"
)

type DB struct {
	st *store.Store
	mu sync.Mutex
}

func Open(dir string) (*DB, error) {

	st, err := store.Open(dir, 1*1024*1024*1024*1024)
	if err != nil {
		return nil, err
	}

	if st.GetRootAddress() == store.NilAddress {
		rootAddress, err := btree.CreateEmptyBTree(st, 3, 32)
		if err != nil {
			return nil, errors.Wrap(err, "while creating empty root btree")
		}

		err = st.SetRootAddress(rootAddress)
		if err != nil {
			return nil, errors.Wrap(err, "while setting root address")
		}
	}

	return &DB{
		st: st,
	}, nil

}

func (d *DB) Close() error {
	return d.st.Close()
}

func (d *DB) NewWriteTransaction() (*WriteTransaction, error) {
	// TODO locking, context, one write tx at a time
	st, err := d.st.PrivateMMap()
	if err != nil {
		return nil, errors.Wrap(err, "while creating private MMAP for read tx")
	}

	return &WriteTransaction{
		s: st,
	}, nil
}
