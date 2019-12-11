package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuilder(t *testing.T) {
	t.Run("nil config", func(t *testing.T) {
		store, err := NewBuilder(nil).StoreByName("store")
		assert.Nil(t, store)
		assert.NotNil(t, err)
	})
	t.Run("missing store stanza", func(t *testing.T) {
		var config map[string]interface{}
		require.Nil(t, toml.Unmarshal([]byte(`[another-store]
type = "in-memory"
`), &config))
		store, err := NewBuilder(config).StoreByName("store")
		assert.Nil(t, store)
		assert.NotNil(t, err)
	})
	t.Run("missing store type", func(t *testing.T) {
		var config map[string]interface{}
		require.Nil(t, toml.Unmarshal([]byte(`[store]
file = "$HOME/lib/dino/testing.db"
`), &config))
		store, err := NewBuilder(config).StoreByName("store")
		assert.Nil(t, store)
		assert.NotNil(t, err)
	})
	t.Run("missing store builder", func(t *testing.T) {
		var config map[string]interface{}
		require.Nil(t, toml.Unmarshal([]byte(`[store]
type = "without-builder"
`), &config))
		store, err := NewBuilder(config).StoreByName("store")
		assert.Nil(t, store)
		assert.NotNil(t, err)
	})
	t.Run("store not a map", func(t *testing.T) {
		var config map[string]interface{}
		require.Nil(t, toml.Unmarshal([]byte(`store = 666
`), &config))
		store, err := NewBuilder(config).StoreByName("store")
		assert.Nil(t, store)
		assert.NotNil(t, err)
	})
	t.Run("store type not a string", func(t *testing.T) {
		var config map[string]interface{}
		require.Nil(t, toml.Unmarshal([]byte(`[store]
type = 666
`), &config))
		store, err := NewBuilder(config).StoreByName("store")
		assert.Nil(t, store)
		assert.NotNil(t, err)
	})
	t.Run("errors are cached", func(t *testing.T) {
		registerBuilder("faulty", func(*Builder, map[string]interface{}) (Store, error) {
			return nil, fmt.Errorf("error@%d", time.Now().UnixNano())
		})
		var config map[string]interface{}
		require.Nil(t, toml.Unmarshal([]byte(`[store]
type = "faulty"
`), &config))
		builder := NewBuilder(config)
		store, err1 := builder.StoreByName("store")
		assert.Nil(t, store)
		assert.NotNil(t, err1)
		store, err2 := builder.StoreByName("store")
		assert.Nil(t, store)
		assert.NotNil(t, err2)
		assert.Equal(t, err1, err2)
	})
}
