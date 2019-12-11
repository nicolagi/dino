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

func TestDiskStoreBuilder(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer func() {
		_ = os.RemoveAll(dir)
	}()

	var config map[string]interface{}
	require.Nil(t, toml.Unmarshal([]byte(fmt.Sprintf(`[disk-store]
type = "disk"
dir = %q
`, dir)), &config))
	store, err := storage.NewBuilder(config).StoreByName("disk-store")
	require.Nil(t, err)
	require.NotNil(t, store)
	_, ok := store.(*storage.DiskStore)
	assert.True(t, ok)
}
