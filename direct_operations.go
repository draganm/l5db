package l5db

import (
	serrors "errors"
	"io/ioutil"

	"github.com/draganm/l5db/btree"
	"github.com/draganm/l5db/dbpath"
	"github.com/draganm/l5db/sequential"
	"github.com/draganm/l5db/store"
	"github.com/pkg/errors"
)

var ErrNotFound = serrors.New("not found")

func (d *DB) getAddressOfParent(parsedPath []string) (store.Address, error) {
	ma := d.st.GetRootAddress()

	for _, pe := range parsedPath[:len(parsedPath)-1] {
		var err error
		ma, err = btree.Get(d.st, ma, []byte(pe))
		if err != nil {
			return store.NilAddress, errors.Wrap(err, "while creating map")
		}
	}

	return ma, nil

}

func (d *DB) CreateMap(pth string) error {

	parsedPath, err := dbpath.Split(pth)
	if err != nil {
		return errors.Wrapf(err, "while parsing dbpath %q", pth)
	}

	if len(pth) == 0 {
		return errors.New("trying to create root")
	}

	ma, err := d.getAddressOfParent(parsedPath)
	if err != nil {
		return err
	}

	empty, err := btree.CreateEmptyBTree(d.st, 5, 32)
	if err != nil {
		return errors.Wrap(err, "while creating empty btree")
	}

	lastKey := parsedPath[len(parsedPath)-1]

	return btree.Put(d.st, ma, []byte(lastKey), empty)

}

func (d *DB) getAddressOf(pth string) (store.Address, error) {
	parsedPath, err := dbpath.Split(pth)
	if err != nil {
		return store.NilAddress, errors.Wrapf(err, "while parsing dbpath %q", pth)
	}

	ma := d.st.GetRootAddress()

	for _, pe := range parsedPath {
		var err error
		ma, err = btree.Get(d.st, ma, []byte(pe))
		if err != nil {
			return store.NilAddress, errors.Wrap(err, "while getting element")
		}
	}

	return ma, nil

}

func (d *DB) Size(path string) (uint64, error) {
	ta, err := d.getAddressOf(path)
	if err != nil {
		return 0, err
	}

	return btree.Count(d.st, ta)
}

func (d *DB) Exists(path string) (bool, error) {
	a, err := d.getAddressOf(path)

	cause := errors.Cause(err)

	if cause == btree.ErrNotFound {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return a != store.NilAddress, nil
}

func (d *DB) Put(pth string, data []byte) error {
	parsedPath, err := dbpath.Split(pth)
	if err != nil {
		return errors.Wrapf(err, "while parsing dbpath %q", pth)
	}

	if len(pth) == 0 {
		return errors.New("trying to put data into root")
	}

	ma, err := d.getAddressOfParent(parsedPath)
	if err != nil {
		return err
	}

	blockSize := 16 * 1024

	if len(data) < blockSize {
		blockSize = len(data)
	}

	empty, err := sequential.CreateEmpty(d.st, uint16(blockSize))
	if err != nil {
		return errors.Wrap(err, "while creating empty sequential data")
	}

	lastKey := parsedPath[len(parsedPath)-1]

	err = sequential.Append(d.st, empty, data)
	if err != nil {
		return errors.Wrap(err, "while appending sequential data")
	}

	return btree.Put(d.st, ma, []byte(lastKey), empty)
}

func (d *DB) Get(path string) ([]byte, error) {
	a, err := d.getAddressOf(path)

	if err != nil {
		return nil, err
	}

	r, err := sequential.Reader(d.st, a)
	if err != nil {
		return nil, err
	}

	return ioutil.ReadAll(r)

}
