package sequential

import (
	"bytes"
	"io"

	"github.com/draganm/l5db/store"
	"github.com/pkg/errors"
)

func Size(m store.Memory, a store.Address) (uint64, error) {
	met, err := getMeta(m, a)
	if err != nil {
		return 0, err
	}

	return met.dataSize(), nil
}

func Append(m store.Memory, a store.Address, data []byte) error {
	met, err := getMeta(m, a)
	if err != nil {
		return err
	}

	return met.append(data)

}

func CreateEmpty(m store.Memory, blockSize uint16) (store.Address, error) {
	a, _, err := createMeta(m, blockSize)
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while creating sequential meta block")
	}

	return a, nil

}

func Reader(m store.Memory, a store.Address) (io.Reader, error) {
	met, err := getMeta(m, a)
	if err != nil {
		return nil, err
	}

	if met.isEmpty() {
		return bytes.NewReader(nil), nil
	}

	return met.reader()

}
