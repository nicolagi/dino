package storage_test

import (
	"bytes"
	"errors"
	"flag"
	"io/ioutil"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/boltdb/bolt"
	"github.com/nicolagi/dino/message"
	"github.com/nicolagi/dino/metadata/client"
	"github.com/nicolagi/dino/metadata/server"
	"github.com/nicolagi/dino/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	dynamodbparams string
	s3params       string
)

func TestStoreImplementations(t *testing.T) {
	testCases := []struct {
		name  string
		setup func(*testing.T) (storage.Store, func())
	}{
		{
			name: "dynamodb",
			setup: func(t *testing.T) (store storage.Store, teardown func()) {
				if dynamodbparams == "" {
					t.Skip()
				}
				t.Helper()
				parts := strings.SplitN(dynamodbparams, ",", 3)
				profile := parts[0]
				region := parts[1]
				table := parts[2]
				ddbs, err := storage.NewDynamoDBStore(
					profile,
					region,
					table,
				)
				if err != nil {
					t.Fatalf("Could not build DynamoDBStore: %v", err)
				}
				return ddbs, func() {
				}
			},
		},
		{
			name: "Store implementation backed by S3",
			setup: func(t *testing.T) (s storage.Store, teardown func()) {
				if s3params == "" {
					t.Skip()
				}
				t.Helper()
				parts := strings.SplitN(s3params, ",", 3)
				profile := parts[0]
				region := parts[1]
				bucket := parts[2]
				store, err := storage.NewS3Store(profile, region, bucket)
				if err != nil {
					t.Fatal(err)
				}
				return store, func() {}
			},
		},
		{
			name: "Store implementation backed by a BoltDB",
			setup: func(t *testing.T) (s storage.Store, teardown func()) {
				f, err := ioutil.TempFile("", "test-dino-storage-")
				require.Nil(t, err)
				require.Nil(t, f.Close())
				db, err := bolt.Open(f.Name(), 0600, nil)
				require.Nil(t, err)
				store, err := storage.NewBoltStore(db)
				require.Nil(t, err)
				return store, func() {
					_ = db.Close()
					_ = os.Remove(f.Name())
				}
			},
		},
		{
			name: "Store implementation backed by a map",
			setup: func(*testing.T) (s storage.Store, teardown func()) {
				return storage.NewInMemoryStore(), func() {
					// Nothing to do.
				}
			},
		},
		{
			name: "Store implementation backed by a host filesystem directory",
			setup: func(t *testing.T) (s storage.Store, teardown func()) {
				dir, err := ioutil.TempDir("", "test-dino-storage-")
				require.Nil(t, err)
				return storage.NewDiskStore(dir), func() {
					_ = os.RemoveAll(dir)
				}
			},
		},
		{
			name: "Paired store backed by two in-memory stores",
			setup: func(t *testing.T) (s storage.Store, teardown func()) {
				return storage.NewPaired(
					storage.NewInMemoryStore(),
					storage.NewInMemoryStore(),
				), func() {}
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			store, teardown := tc.setup(t)
			defer teardown()
			testStore(t, store)
		})
	}
}

func TestVersionedStoreImplementations(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	testCases := []struct {
		name  string
		setup func(*testing.T) (storage.VersionedStore, func())
	}{
		{
			name: "remote",
			setup: func(t *testing.T) (store storage.VersionedStore, teardown func()) {
				remoteServer := server.New(
					server.WithAddress("localhost:0"),
					server.WithVersionedStore(storage.NewVersionedWrapper(storage.NewInMemoryStore())),
				)
				remoteAddress, err := remoteServer.Listen()
				require.Nil(t, err)
				srvc := make(chan struct{})
				go func() {
					assert.Nil(t, remoteServer.Serve())
					close(srvc)
				}()
				remoteClient := client.New(client.WithAddress(remoteAddress))
				remoteStore := storage.NewRemoteVersionedStore(remoteClient)
				remoteStore.Start()
				return remoteStore, func() {
					remoteStore.Stop()
					assert.Nil(t, remoteServer.Shutdown())
					<-srvc
				}
			},
		},
		{
			name: "dynamodb",
			setup: func(t *testing.T) (store storage.VersionedStore, teardown func()) {
				if dynamodbparams == "" {
					t.Skip()
				}
				t.Helper()
				parts := strings.SplitN(dynamodbparams, ",", 3)
				profile := parts[0]
				region := parts[1]
				table := parts[2]
				ddbvs, err := storage.NewDynamoDBVersionedStore(
					profile,
					region,
					table,
				)
				if err != nil {
					t.Fatalf("Could not build DynamoDBVersionedStore: %v", err)
				}
				return ddbvs, func() {
				}
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			store, teardown := tc.setup(t)
			defer teardown()
			testVersionedStore(t, store)
		})
	}
}

func testStore(t *testing.T, store storage.Store) {
	rand.Seed(time.Now().UnixNano())
	t.Run("what you put is what you get", func(t *testing.T) {
		key := randomKey()
		err := store.Put(key, []byte("hello"))
		require.Nil(t, err)
		storedValue, err := store.Get(key)
		require.Nil(t, err)
		assert.Equal(t, []byte("hello"), storedValue)
	})
	t.Run("error on not existing key", func(t *testing.T) {
		key := randomKey()
		value, err := store.Get(key)
		assert.True(t, errors.Is(err, storage.ErrNotFound))
		assert.Nil(t, value)
	})
	t.Run("can put a nil value, get non-nil empty slice", func(t *testing.T) {
		key := randomKey()
		err := store.Put(key, nil)
		require.Nil(t, err)
		value, err := store.Get(key)
		assert.Nil(t, err)
		assert.Equal(t, []byte{}, value)
	})
	t.Run("can put an empty value", func(t *testing.T) {
		key := randomKey()
		err := store.Put(key, []byte{})
		require.Nil(t, err)
		value, err := store.Get(key)
		assert.Nil(t, err)
		assert.Equal(t, []byte{}, value)
	})
	t.Run("mutating value should not affect stored pairs", func(t *testing.T) {
		key := randomKey()
		before := []byte("old value")
		if err := store.Put(key, before); err != nil {
			t.Fatalf("got %v, want nil", err)
		}
		copy(before, "new")
		after, err := store.Get(key)
		if err != nil {
			t.Fatalf("got %v, want nil", err)
		}
		if want := []byte("old value"); !bytes.Equal(want, after) {
			t.Errorf("got %q, want %q", after, want)
		}
	})
	t.Run("mutating key should not cause a race condition", func(t *testing.T) {
		key := randomKey()
		value := []byte("value")
		if err := store.Put(key, value); err != nil {
			t.Fatalf("got %v, want nil", err)
		}
		copy(key, "other")
	})
	t.Run("corresponding versioned store", func(t *testing.T) {
		vs := storage.NewVersionedWrapper(store)
		testVersionedStore(t, vs)
	})
}

func testVersionedStore(t *testing.T, vs storage.VersionedStore) {
	t.Run("error on getting non existing key", func(t *testing.T) {
		key := randomKey()
		version, value, err := vs.Get(key)
		assert.EqualValues(t, 0, version)
		assert.Nil(t, value)
		if !assert.True(t, errors.Is(err, storage.ErrNotFound)) {
			t.Logf("unwanted error: %v", err)
		}
	})
	t.Run("accepts any initial version for new pairs", func(t *testing.T) {
		key := randomKey()
		version := message.RandomVersion()
		value := message.RandomBytes()
		err := vs.Put(version, key, value)
		assert.Nil(t, err)
		storedVersion, storedValue, err := vs.Get(key)
		assert.Equal(t, version, storedVersion)
		assert.EqualValues(t, value, storedValue)
		assert.Nil(t, err)
	})
	t.Run("rejects stale puts", func(t *testing.T) {
		key := randomKey()
		err := vs.Put(0, key, []byte("hello"))
		require.Nil(t, err)
		err = vs.Put(0, key, []byte("goodbye"))
		assert.Equal(t, storage.ErrStalePut, err)
		version, storedValue, err := vs.Get(key)
		require.Nil(t, err)
		assert.EqualValues(t, 0, version)
		assert.Equal(t, []byte("hello"), storedValue)
	})
	t.Run("puts increase version number", func(t *testing.T) {
		key := randomKey()
		require.Nil(t, vs.Put(0, key, []byte("hello")))
		require.Nil(t, vs.Put(1, key, []byte("goodbye")))
		version, storedValue, err := vs.Get(key)
		require.Nil(t, err)
		assert.EqualValues(t, 1, version)
		assert.Equal(t, []byte("goodbye"), storedValue)
	})
}

func randomKey() []byte {
	key := make([]byte, 128)
	rand.Read(key)
	return key
}

func TestMain(m *testing.M) {
	flag.StringVar(&dynamodbparams, "dynamodb", "", "profile, region, table name for DynamoDB versioned store testing")
	flag.StringVar(&s3params, "s3", "", "profile, region, bucket for S3 store testing")
	flag.Parse()
	os.Exit(m.Run())
}
