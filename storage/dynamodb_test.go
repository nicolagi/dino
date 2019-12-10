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

func TestDynamoDBStoreBuilder(t *testing.T) {
	if dynamodbparams == "" {
		t.Skip()
	}
	parts := strings.SplitN(dynamodbparams, ",", 3)
	var config map[string]interface{}
	require.Nil(t, toml.Unmarshal([]byte(fmt.Sprintf(`[dynamodb-store]
type = "dynamodb"
profile = %q
region = %q
table = %q
`, parts[0], parts[1], parts[2])), &config))
	store, err := storage.NewBuilder(config).StoreByName("dynamodb-store")
	require.Nil(t, err)
	require.NotNil(t, store)
	_, ok := store.(*storage.DynamoDBStore)
	assert.True(t, ok)
}
