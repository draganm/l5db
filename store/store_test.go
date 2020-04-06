package store_test

import (
	"io/ioutil"
	"os"
	"testing"

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

func TestOpenEmptyStore(t *testing.T) {
	td, cleanup := tempDir(t)
	defer cleanup()

	st, err := store.Open(td, 1024)
	require.NoError(t, err)

	err = st.Close()
	require.NoError(t, err)
}

func TestAllocate(t *testing.T) {
	td, cleanup := tempDir(t)
	defer cleanup()

	st, err := store.Open(td, 1024)
	require.NoError(t, err)

	addr, bl, err := st.Allocate(3)
	require.NoError(t, err)
	require.Equal(t, 3, len(bl.Data()))
	require.Equal(t, uint64(16), addr)

	copy(bl.Data(), []byte{1, 2, 3})

	err = st.Close()
	require.NoError(t, err)

	st, err = store.Open(td, 1024)
	require.NoError(t, err)
	defer st.Close()

	bl = st.GetBlock(addr)

	require.Equal(t, []byte{1, 2, 3}, bl.Data())

}
