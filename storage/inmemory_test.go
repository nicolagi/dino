package storage_test

import (
	"fmt"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/nicolagi/dino/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryStoreBuilder(t *testing.T) {
	var config map[string]interface{}
	require.Nil(t, toml.Unmarshal([]byte(fmt.Sprintf(`[in-memory-store]
type = "in-memory"
`)), &config))
	store, err := storage.NewBuilder(config).StoreByName("in-memory-store")
	require.Nil(t, err)
	require.NotNil(t, store)
	_, ok := store.(*storage.InMemoryStore)
	assert.True(t, ok)
}
