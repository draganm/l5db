package btree_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/draganm/l5db/btree"
	"github.com/draganm/l5db/store"
	"github.com/stretchr/testify/require"
)

func tempDir(t *testing.T) (string, func()) {
	d, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	return d, func() {
		os.RemoveAll(d)
	}
}

func createTestStore(t *testing.T) (*store.Store, func()) {
	td, cleanup := tempDir(t)
	// defer cleanup()

	st, err := store.Open(td, 1024*1024*1024)
	require.NoError(t, err)

	return st, func() {
		err = st.Close()
		require.NoError(t, err)
		cleanup()
	}

}

func TestCreateEmptyBTRee(t *testing.T) {
	ts, cleanup := createTestStore(t)
	defer cleanup()

	a, err := btree.CreateEmptyBTree(ts, 2, 32)
	require.NoError(t, err)
	require.NotEqual(t, store.NilAddress, a)
}
