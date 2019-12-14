package server_test

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/nicolagi/dino/message"
	"github.com/nicolagi/dino/metadata/client"
	"github.com/nicolagi/dino/metadata/server"
	"github.com/nicolagi/dino/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServer(t *testing.T) {
	t.Run("can be shutdown right after start", func(t *testing.T) {
		_, cleanup := newDisposableServer(t)
		defer cleanup()
	})
	t.Run("send error message", func(t *testing.T) {
		address, cleanup := newDisposableServer(t)
		defer cleanup()
		c := newAttachedClient(address)
		request := message.NewErrorMessage(431, "test error")
		assert.Nil(t, c.Send(request))
		var response message.Message
		require.Nil(t, c.Receive(&response))
		assert.Equal(t, message.KindError, response.Kind())
		assert.Equal(t, "messages of kind ERROR cannot be applied", response.Value())
	})
	t.Run("send auth message", func(t *testing.T) {
		address, cleanup := newDisposableServer(t)
		defer cleanup()
		c := newAttachedClient(address)
		request := message.NewAuthMessage(439, "password")
		assert.Nil(t, c.Send(request))
		var response message.Message
		require.Nil(t, c.Receive(&response))
		assert.Equal(t, message.KindError, response.Kind())
		assert.Equal(t, "messages of kind AUTH cannot be applied", response.Value())
	})
	t.Run("notify to closed connection", func(t *testing.T) {
		address, cleanup := newDisposableServer(t)
		defer cleanup()

		// Connect two clients.
		c1 := newAttachedClient(address)
		c2 := newAttachedClient(address)

		// Close c2, then send a put via c1.
		// This would attempt a notification on c2.
		c2.Close()
		req1 := message.NewPutMessage(1, "genre", "jazz", 1)
		require.Nil(t, c1.Send(req1))

		// Check req1 got a proper response on c1, despite propagation to c2
		// failing.
		var res1 message.Message
		require.Nil(t, c1.Receive(&res1))
		assert.Equal(t, req1, res1)
	})
	t.Run("one client puts, another client puts, conflict", func(t *testing.T) {
		address, cleanup := newDisposableServer(t)
		defer cleanup()

		// Send conflicting puts from two clients.
		client1, _ := newRemoteVersionedStore(address)
		client2, _ := newRemoteVersionedStore(address)
		err1 := client1.Put(1, []byte("name"), []byte("Alberto"))
		err2 := client2.Put(1, []byte("name"), []byte("Leonardo"))

		var winner []byte
		if err1 != nil {
			assert.Equal(t, err1, storage.ErrStalePut)
			assert.Nil(t, err2)
			winner = []byte("Leonardo")
		} else {
			assert.Equal(t, err2, storage.ErrStalePut)
			winner = []byte("Alberto")
		}

		version1, value1, err1 := client1.Get([]byte("name"))
		version2, value2, err2 := client2.Get([]byte("name"))
		assert.Nil(t, err1)
		assert.Nil(t, err2)
		assert.EqualValues(t, 1, version1)
		assert.EqualValues(t, 1, version2)
		assert.Equal(t, winner, value1)
		assert.Equal(t, winner, value2)
	})
	t.Run("one client puts, another one gets", func(t *testing.T) {
		address, cleanup := newDisposableServer(t)
		defer cleanup()

		// Use one client to connect and put a key.
		vs1, _ := newRemoteVersionedStore(address)
		err := vs1.Put(1, []byte("username"), []byte("glenda"))
		require.Nil(t, err)

		// Connect a new client, get, receive what was put by first client.
		vs2, _ := newRemoteVersionedStore(address)
		version, value, err := vs2.Get([]byte("username"))
		require.Nil(t, err)
		assert.EqualValues(t, 1, version)
		assert.EqualValues(t, "glenda", value)
	})
	t.Run("successful put fans out to many clients", func(t *testing.T) {
		address, cleanup := newDisposableServer(t)

		// Connect three clients.
		vs1, _ := newRemoteVersionedStore(address)
		vs2, ready2 := newRemoteVersionedStore(address)
		vs3, ready3 := newRemoteVersionedStore(address)

		// One client makes a succesful put.
		require.Nil(t, vs1.Put(444, []byte("foo"), []byte("bar")))

		<-ready2
		<-ready3
		cleanup()

		// All clients know *locally* about the value of "foo".
		verify := func(rvs *storage.RemoteVersionedStore) {
			version, value, err := rvs.Get([]byte("foo"))
			if err != nil {
				t.Errorf("got %v, want nil", err)
			}
			if want := []byte("bar"); !bytes.Equal(value, want) {
				t.Errorf("got %q, want %q", value, want)
			}
			if want := uint64(444); version != want {
				t.Errorf("got %d, want %d", version, want)
			}
		}

		verify(vs2)
		verify(vs3)
	})
	t.Run("should not allow a password to be transmitted in cleartext", func(t *testing.T) {
		s := server.New(server.WithAuthHash("anything"))
		_, err := s.Listen()
		assert.True(t, errors.Is(err, server.ErrPasswordWithoutTLS))
	})
}

func newDisposableServer(t *testing.T) (address string, cleanup func()) {
	store := storage.NewInMemoryStore()
	versionedStore := storage.NewVersionedWrapper(store)
	metadataServer := server.New(
		server.WithAddress("localhost:0"),
		server.WithVersionedStore(versionedStore),
	)
	address, err := metadataServer.Listen()
	require.Nil(t, err)
	errc := make(chan error, 1)
	go func() {
		errc <- metadataServer.Serve()
	}()
	return address, func() {
		assert.Nil(t, metadataServer.Shutdown())
		assert.Nil(t, <-errc)
	}
}

func newAttachedClient(address string) (c *client.Client) {
	return client.New(client.WithAddress(address))
}

func newRemoteVersionedStore(address string) (*storage.RemoteVersionedStore, chan message.Message) {
	recv := make(chan message.Message, 1)
	vs := storage.NewRemoteVersionedStore(
		client.New(client.WithAddress(address)),
		storage.WithRequestTimeout(5*time.Second),
		storage.WithChangeListener(func(m message.Message) {
			recv <- m
		}),
	)
	vs.Start()
	return vs, recv
}
