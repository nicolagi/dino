package storage_test

import (
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/nicolagi/dino/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRemoteStoreBuilder(t *testing.T) {
	var config map[string]interface{}
	require.Nil(t, toml.Unmarshal([]byte(`[remote-store]
type = "remote"
address = "dino.example.org:7777"
`), &config))
	store, err := storage.NewBuilder(config).StoreByName("remote-store")
	require.Nil(t, err)
	require.NotNil(t, store)
	_, ok := store.(*storage.RemoteStore)
	assert.True(t, ok)
}
