package l5db_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/draganm/l5db"
	"github.com/stretchr/testify/require"
)

func createTempDir(t *testing.T) (string, func()) {
	d, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	return d, func() {
		os.RemoveAll(d)
	}
}

func TestOpenAndClose(t *testing.T) {
	td, cleanup := createTempDir(t)
	defer cleanup()

	db, err := l5db.Open(td)
	require.NoError(t, err)

	err = db.Close()
	require.NoError(t, err)

}

func createEmptyDB(t *testing.T) (*l5db.DB, func()) {
	td, cleanup := createTempDir(t)
	db, err := l5db.Open(td)
	require.NoError(t, err)

	return db, func() {
		err = db.Close()
		require.NoError(t, err)
		cleanup()
	}
}

func TestCreateEmptyMap(t *testing.T) {
	db, cleanup := createEmptyDB(t)

	defer cleanup()

	err := db.CreateMap("abc")
	require.NoError(t, err)

	rootSize, err := db.Size("")
	require.NoError(t, err)
	require.Equal(t, uint64(1), rootSize)

	abcSize, err := db.Size("abc")
	require.NoError(t, err)
	require.Equal(t, uint64(0), abcSize)

}
