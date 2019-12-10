package main

import (
	"github.com/nicolagi/dino/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestConfig(t *testing.T) {
	toml := `debug = true
listen_address = ":6660"
cert_file = "some cert file"
key_file = "some key file"
backend = "backend"

[stores]

[stores.backend]
type = "paired"
slow = "slow"
fast = "fast"

[stores.slow]
type = "boltdb"
file = "$HOME/lib/dino/metadata.db"

[stores.fast]
type = "in-memory"
`
	opts, err := loadOptions(strings.NewReader(toml))
	require.Nil(t, err)
	t.Run("basic properties are set", func(t *testing.T) {
		assert.True(t, opts.Debug)
		assert.Equal(t, ":6660", opts.ListenAddress)
		assert.Equal(t, "some cert file", opts.CertFile)
		assert.Equal(t, "some key file", opts.KeyFile)
		assert.Equal(t, "backend", opts.Backend)
	})
	t.Run("can create store", func(t *testing.T) {
		store, err := storage.NewBuilder(opts.Stores).StoreByName(opts.Backend)
		require.Nil(t, err)
		require.NotNil(t, store)
		_, ok := store.(storage.Paired)
		assert.True(t, ok)
	})
}
