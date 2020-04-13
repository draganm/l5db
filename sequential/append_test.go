package sequential_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/draganm/l5db/sequential"
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
	defer cleanup()

	st, err := store.Open(td, 1024*1024*1024)
	require.NoError(t, err)

	return st, func() {
		err = st.Close()
		require.NoError(t, err)
		cleanup()
	}

}

func TestAppendToEmpty(t *testing.T) {

	ts, cleanup := createTestStore(t)
	defer cleanup()

	a, err := sequential.CreateEmpty(ts, 32)
	require.NoError(t, err)

	err = sequential.Append(ts, a, []byte{1, 2, 3})
	require.NoError(t, err)

	s, err := sequential.Size(ts, a)
	require.NoError(t, err)

	require.Equal(t, uint64(3), s)

	r, err := sequential.Reader(ts, a)
	require.NoError(t, err)

	d, err := ioutil.ReadAll(r)
	require.NoError(t, err)

	require.Equal(t, []byte{1, 2, 3}, d)

}

func TestAppendToFullBlock(t *testing.T) {

	ts, cleanup := createTestStore(t)
	defer cleanup()

	a, err := sequential.CreateEmpty(ts, 3)
	require.NoError(t, err)

	err = sequential.Append(ts, a, []byte{1, 2, 3})
	require.NoError(t, err)

	s, err := sequential.Size(ts, a)
	require.NoError(t, err)

	require.Equal(t, uint64(3), s)

	r, err := sequential.Reader(ts, a)
	require.NoError(t, err)

	d, err := ioutil.ReadAll(r)
	require.NoError(t, err)

	require.Equal(t, []byte{1, 2, 3}, d)

}

func TestAppendTreeBlocks(t *testing.T) {

	ts, cleanup := createTestStore(t)
	defer cleanup()

	a, err := sequential.CreateEmpty(ts, 1)
	require.NoError(t, err)

	err = sequential.Append(ts, a, []byte{1, 2, 3})
	require.NoError(t, err)

	s, err := sequential.Size(ts, a)
	require.NoError(t, err)

	require.Equal(t, uint64(3), s)

	r, err := sequential.Reader(ts, a)
	require.NoError(t, err)

	d, err := ioutil.ReadAll(r)
	require.NoError(t, err)

	require.Equal(t, []byte{1, 2, 3}, d)

	sz, err := sequential.Size(ts, a)
	require.NoError(t, err)
	require.Equal(t, uint64(3), sz)

}
