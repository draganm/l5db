package store

import "encoding/binary"

type Block []byte

func newBlock(d []byte) Block {
	binary.BigEndian.PutUint16(d, uint16(len(d)))
	return d
}

func (b Block) Data() []byte {
	return b[2:]
}

func toBlock(d []byte) Block {
	len := binary.BigEndian.Uint16(d)
	return d[:len]
}
