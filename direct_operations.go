package l5db

import (
	serrors "errors"

	"github.com/draganm/l5db/btree"
	"github.com/draganm/l5db/dbpath"
	"github.com/pkg/errors"
)

var ErrNotFound = serrors.New("not found")

func (d *DB) CreateMap(pth string) error {

	parsedPath, err := dbpath.Split(pth)
	if err != nil {
		return errors.Wrapf(err, "while parsing dbpath %q", pth)
	}

	if len(pth) == 0 {
		return errors.New("trying to create root")
	}

	ma := d.st.GetRootAddress()

	for _, pe := range parsedPath[:len(parsedPath)-1] {
		ma, err = btree.Get(d.st, ma, []byte(pe))
		if err != nil {
			return errors.Wrap(err, "while creating map")
		}
	}

	empty, err := btree.CreateEmptyBTree(d.st, 5, 32)
	if err != nil {
		return errors.Wrap(err, "while creating empty btree")
	}

	lastKey := parsedPath[len(parsedPath)-1]

	return btree.Put(d.st, ma, []byte(lastKey), empty)

}
