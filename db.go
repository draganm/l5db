package l5db

import (
	"github.com/draganm/l5db/btree"
	"github.com/draganm/l5db/store"
	"github.com/pkg/errors"
)

type DB struct {
	st *store.Store
}

func Open(dir string) (*DB, error) {

	st, err := store.Open(dir, 2*1024*1024*1024*1024)
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