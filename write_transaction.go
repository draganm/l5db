package l5db

import (
	"io/ioutil"

	"github.com/draganm/l5db/btree"
	"github.com/draganm/l5db/dbpath"
	"github.com/draganm/l5db/sequential"
	"github.com/draganm/l5db/store"
	"github.com/pkg/errors"
)

type WriteTransaction struct {
	s *store.Store
}

func (d *WriteTransaction) getAddressOfParent(parsedPath []string) (store.Address, error) {

	ma := d.s.GetRootAddress()

	for _, pe := range parsedPath[:len(parsedPath)-1] {
		var err error
		ma, err = btree.Get(d.s, ma, []byte(pe))
		if err != nil {
			return store.NilAddress, errors.Wrap(err, "while creating map")
		}
	}

	return ma, nil

}

func (d *WriteTransaction) CreateMap(pth string) error {

	parsedPath, err := dbpath.Split(pth)
	if err != nil {
		return errors.Wrapf(err, "while parsing dbpath %q", pth)
	}

	if len(pth) == 0 {
		return errors.New("trying to create root")
	}

	lastKey := parsedPath[len(parsedPath)-1]

	ma, err := d.getAddressOfParent(parsedPath)
	if err != nil {
		return err
	}

	empty, err := btree.CreateEmptyBTree(d.s, 5, 32)
	if err != nil {
		return errors.Wrap(err, "while creating empty btree")
	}

	return btree.Put(d.s, ma, []byte(lastKey), empty)

}

func (d *WriteTransaction) getAddressOf(pth string) (store.Address, error) {
	parsedPath, err := dbpath.Split(pth)
	if err != nil {
		return store.NilAddress, errors.Wrapf(err, "while parsing dbpath %q", pth)
	}

	ma := d.s.GetRootAddress()

	for _, pe := range parsedPath {
		var err error
		ma, err = btree.Get(d.s, ma, []byte(pe))
		if err != nil {
			return store.NilAddress, errors.Wrap(err, "while getting element")
		}
	}

	return ma, nil

}

func (d *WriteTransaction) Size(path string) (uint64, error) {

	ta, err := d.getAddressOf(path)
	if err != nil {
		return 0, err
	}

	return btree.Count(d.s, ta)
}

func (d *WriteTransaction) Exists(path string) (bool, error) {

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

func (d *WriteTransaction) Put(pth string, data []byte) error {

	parsedPath, err := dbpath.Split(pth)
	if err != nil {
		return errors.Wrapf(err, "while parsing dbpath %q", pth)
	}

	if len(pth) == 0 {
		return errors.New("trying to put data into root")
	}

	lastKey := parsedPath[len(parsedPath)-1]

	ma, err := d.getAddressOfParent(parsedPath)
	if err != nil {
		return err
	}

	blockSize := 16 * 1024

	if len(data) < blockSize {
		blockSize = len(data)
	}

	empty, err := sequential.CreateEmpty(d.s, uint16(blockSize))
	if err != nil {
		return errors.Wrap(err, "while creating empty sequential data")
	}

	err = sequential.Append(d.s, empty, data)
	if err != nil {
		return errors.Wrap(err, "while appending sequential data")
	}

	return btree.Put(d.s, ma, []byte(lastKey), empty)
}

func (d *WriteTransaction) Get(path string) ([]byte, error) {

	a, err := d.getAddressOf(path)

	if err != nil {
		return nil, err
	}

	r, err := sequential.Reader(d.s, a)
	if err != nil {
		return nil, err
	}

	return ioutil.ReadAll(r)

}
