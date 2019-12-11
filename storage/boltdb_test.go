package storage_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/nicolagi/dino/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBoltStoreBuilder(t *testing.T) {
	f, err := ioutil.TempFile("", "")
	require.Nil(t, err)
	require.Nil(t, f.Close())
	defer func() {
		_ = os.Remove(f.Name())
	}()

	var config map[string]interface{}
	require.Nil(t, toml.Unmarshal([]byte(fmt.Sprintf(`[boltdb-store]
type = "boltdb"
file = %q
`, f.Name())), &config))
	store, err := storage.NewBuilder(config).StoreByName("boltdb-store")
	require.Nil(t, err)
	require.NotNil(t, store)
	_, ok := store.(*storage.BoltStore)
	assert.True(t, ok)
}
