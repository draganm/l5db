package l5db_test

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteTransactionIsolation(t *testing.T) {
	db, cleanup := createEmptyDB(t)
	defer cleanup()

	err := db.CreateMap("abc")
	require.NoError(t, err)

	ex, err := db.Exists("abc")
	require.NoError(t, err)
	require.True(t, ex)

	wtx, err := db.NewWriteTransaction()
	require.NoError(t, err)

	err = wtx.CreateMap("def")
	require.NoError(t, err)

	ex, err = wtx.Exists("abc")
	require.NoError(t, err)
	require.True(t, ex)

	ex, err = wtx.Exists("def")
	require.NoError(t, err)
	require.True(t, ex)

	ex, err = db.Exists("def")
	require.NoError(t, err)
	require.False(t, ex)

}
