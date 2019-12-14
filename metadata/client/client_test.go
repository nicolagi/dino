package client_test

import (
	"math/rand"
	"testing"
	"time"

	"github.com/nicolagi/dino/message"
	"github.com/nicolagi/dino/metadata/client"
	"github.com/nicolagi/dino/metadata/server"
	"github.com/nicolagi/dino/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClient(t *testing.T) {
	var gtor message.Message
	srv := server.New(
		server.WithAddress("localhost:0"),
		server.WithVersionedStore(storage.NewVersionedWrapper(storage.NewInMemoryStore())),
	)
	addr, err := srv.Listen()
	require.Nil(t, err)
	defer func() {
		_ = srv.Shutdown()
	}()
	go func() {
		assert.Nil(t, srv.Serve())
	}()
	t.Run("fails to connect to non-TLS server by default", func(t *testing.T) {
		clnt := client.New(client.WithAddress(addr))
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		m := gtor.Generate(r, 12).Interface().(message.Message)
		err := clnt.Send(m)
		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "first record does not look like a TLS handshake")
	})
	t.Run("falls back to plain TCP if configured to do so", func(t *testing.T) {
		clnt := client.New(client.WithAddress(addr), client.WithFallbackToPlainTCP())
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		m := gtor.Generate(r, 12).Interface().(message.Message)
		err := clnt.Send(m)
		require.Nil(t, err)
		clnt.Close()
	})
}
