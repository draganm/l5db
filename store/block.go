package store

import (
	"encoding/binary"

	"github.com/pkg/errors"
)

type Block []byte

func newBlock(d []byte) Block {
	binary.BigEndian.PutUint16(d, uint16(len(d)))
	return d
}

func (b Block) Data() []byte {
	return b[2:]
}

func toBlock(d []byte) (Block, error) {
	l := binary.BigEndian.Uint16(d)
	if len(d) < int(l) {
		return nil, errors.New("underlying block slice is too short")
	}
	return d[:l], nil
}
