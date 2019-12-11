package storage_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/nicolagi/dino/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestS3StoreBuilder(t *testing.T) {
	if s3params == "" {
		t.Skip()
	}
	parts := strings.SplitN(s3params, ",", 3)
	var config map[string]interface{}
	require.Nil(t, toml.Unmarshal([]byte(fmt.Sprintf(`[s3-store]
type = "s3"
profile = %q
region = %q
bucket = %q
`, parts[0], parts[1], parts[2])), &config))
	store, err := storage.NewBuilder(config).StoreByName("s3-store")
	require.Nil(t, err)
	require.NotNil(t, store)
	_, ok := store.(*storage.S3Store)
	assert.True(t, ok)
}
