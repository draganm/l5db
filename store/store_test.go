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

	addr, err := st.Allocate(3, store.BTreeMetaBlockType)
	require.NoError(t, err)
	require.Equal(t, store.Address(19), addr)

	err = st.Update(addr, []byte{1, 2, 3})
	require.NoError(t, err)

	err = st.Close()
	require.NoError(t, err)

	st, err = store.Open(td, 1024)
	require.NoError(t, err)
	defer st.Close()

	bl, bt, err := st.GetBlock(addr)
	require.NoError(t, err)
	require.Equal(t, store.BTreeMetaBlockType, bt)

	require.Equal(t, []byte{1, 2, 3}, bl)

	t.Run("when I allocate another block", func(t *testing.T) {

		addr2, err := st.Allocate(4, store.BTreeMetaBlockType)
		require.NoError(t, err)

		err = st.Update(addr2, []byte{1, 2, 3, 4})
		require.NoError(t, err)

		bl, bt, err := st.GetBlock(addr2)
		require.NoError(t, err)
		require.Equal(t, store.BTreeMetaBlockType, bt)

		require.Equal(t, []byte{1, 2, 3, 4}, bl)

	})

}
