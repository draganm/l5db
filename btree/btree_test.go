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

func TestPutIntoEmptyTree(t *testing.T) {
	ts, cleanup := createTestStore(t)
	defer cleanup()

	a, err := btree.CreateEmptyBTree(ts, 2, 32)
	require.NoError(t, err)
	require.NotEqual(t, store.NilAddress, a)

	err = btree.Put(ts, a, []byte{1, 2, 3}, store.Address(666))
	require.NoError(t, err)

	ga, err := btree.Get(ts, a, []byte{1, 2, 3})
	require.NoError(t, err)
	require.Equal(t, ga, store.Address(666))

	cnt, err := btree.Count(ts, a)
	require.NoError(t, err)
	require.Equal(t, uint64(1), cnt)
}

func TestSplittingFullLeaf(t *testing.T) {
	ts, cleanup := createTestStore(t)
	defer cleanup()

	a, err := btree.CreateEmptyBTree(ts, 2, 32)
	require.NoError(t, err)
	require.NotEqual(t, store.NilAddress, a)

	t.Run("given the btree's root is a full leaf", func(t *testing.T) {
		err = btree.Put(ts, a, []byte{1, 2, 3}, store.Address(666))
		require.NoError(t, err)
		err = btree.Put(ts, a, []byte{1, 2, 4}, store.Address(667))
		require.NoError(t, err)
		err = btree.Put(ts, a, []byte{1, 2, 5}, store.Address(668))
		require.NoError(t, err)

		cnt, err := btree.Count(ts, a)
		require.NoError(t, err)
		require.Equal(t, uint64(3), cnt)

	})

	t.Run("when I insert another key", func(t *testing.T) {
		err = btree.Put(ts, a, []byte{1, 2, 6}, store.Address(669))
	})

	t.Run("it should not fail", func(t *testing.T) {
		require.NoError(t, err)
	})

	t.Run("it should incremenet the count", func(t *testing.T) {
		cnt, err := btree.Count(ts, a)
		require.NoError(t, err)
		require.Equal(t, uint64(4), cnt)
	})

	t.Run("it should not loose any keys", func(t *testing.T) {
		ga, err := btree.Get(ts, a, []byte{1, 2, 3})
		require.NoError(t, err)
		require.Equal(t, ga, store.Address(666))
	})

}
