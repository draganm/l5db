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
