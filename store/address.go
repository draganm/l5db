package store

type Address uint64

const NilAddress Address = 0

func (a Address) UInt64() uint64 {
	return uint64(a)
}
