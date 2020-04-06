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
