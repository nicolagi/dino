package storage

import (
	"fmt"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPairedStoreBuilder(t *testing.T) {
	var config map[string]interface{}
	require.Nil(t, toml.Unmarshal([]byte(fmt.Sprintf(`[paired-distinct]
type = "paired"
slow = "first"
fast = "second"

[paired-same]
type = "paired"
slow = "first"
fast = "first"

[first]
type = "in-memory"

[second]
type = "in-memory"
`)), &config))

	t.Run("pairing two in-memory stores", func(t *testing.T) {
		store, err := NewBuilder(config).StoreByName("paired-distinct")
		require.Nil(t, err)
		require.NotNil(t, store)
		paired, ok := store.(Paired)
		assert.True(t, ok)
		assert.True(t, paired.fast != paired.slow)
		_, ok1 := paired.fast.(*InMemoryStore)
		_, ok2 := paired.slow.(*InMemoryStore)
		assert.True(t, ok1)
		assert.True(t, ok2)
	})

	t.Run("pairing a store with itself (silly)", func(t *testing.T) {
		store, err := NewBuilder(config).StoreByName("paired-same")
		require.Nil(t, err)
		require.NotNil(t, store)
		paired, ok := store.(Paired)
		assert.True(t, ok)
		assert.True(t, paired.fast == paired.slow)
		_, ok = paired.fast.(*InMemoryStore)
		assert.True(t, ok)
	})
}
