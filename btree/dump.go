package btree

import (
	"encoding/json"

	"github.com/draganm/l5db/store"
)

func Dump(m store.Memory, a store.Address) string {
	mn, err := getMetaNode(m, a)
	if err != nil {
		panic(err)
	}
	st := mn.structure()
	d, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		panic(nil)
	}

	return string(d)
}
