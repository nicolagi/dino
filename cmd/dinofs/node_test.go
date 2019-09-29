package main

import (
	"context"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"

	"github.com/google/gops/agent"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/nicolagi/dino/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeVersionedStore struct {
	mu  sync.Mutex
	err error
}

func (s *fakeVersionedStore) Get([]byte) (uint64, []byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return 0, nil, s.err
}

func (s *fakeVersionedStore) Put(uint64, []byte, []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (s *fakeVersionedStore) setErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}

func TestNodeMetadataRollback(t *testing.T) {
	if err := agent.Listen(agent.Options{}); err != nil {
		t.Logf("Could not start gops agent: %v", err)
	}
	defer agent.Close()
	rootdir, factory, cleanup := testMount(t)
	defer cleanup()
	ko := func() {
		factory.metadata.(*fakeVersionedStore).setErr(errors.New("computer bought the farm"))
	}
	ok := func() {
		factory.metadata.(*fakeVersionedStore).setErr(nil)
	}
	t.Run("Setxattr", func(t *testing.T) {
		t.Run("rolls back additions", func(t *testing.T) {
			node, err := factory.allocNode()
			if err != nil {
				t.Fatal(err)
			}
			ko()
			errno := node.Setxattr(context.Background(), "key", []byte("value"), 0)
			if errno != syscall.EIO {
				t.Fatalf("got %d, want %d", errno, syscall.EIO)
			}
			assert.Len(t, node.xattrs, 0)
		})
		t.Run("rolls back updates", func(t *testing.T) {
			node, err := factory.allocNode()
			require.Nil(t, err)
			ok()
			errno := node.Setxattr(context.Background(), "key", []byte("old value"), 0)
			require.EqualValues(t, 0, errno)
			ko()
			errno = node.Setxattr(context.Background(), "key", []byte("value"), 0)
			if errno != syscall.EIO {
				t.Fatalf("got %d, want %d", errno, syscall.EIO)
			}
			assert.Len(t, node.xattrs, 1)
			assert.EqualValues(t, "old value", node.xattrs["key"])
		})
	})
	t.Run("Rmdir", func(t *testing.T) {
		t.Run("adds back removed child directory", func(t *testing.T) {
			p := filepath.Join(rootdir, "pallina")
			ok()
			err := os.Mkdir(p, 0755)
			if err != nil {
				t.Fatalf("got %v, want nil", err)
			}
			if _, err := os.Stat(p); err != nil {
				t.Fatalf("got %v, want nil", err)
			}
			ko()
			if err := os.Remove(p); err == nil {
				t.Fatal("got nil, want non-nil")
			}
			if _, err := os.Stat(p); err != nil {
				t.Fatalf("got %v, want nil", err)
			}
			// Second remove should succeed, while without rollback it would panic
			// (assuming entry from map non-nil) or return syscall.ENOENT if we're being
			// defensive enough.
			ok()
			err = os.Remove(p)
			if err != nil {
				t.Fatalf("got %v, want nil", err)
			}
			if _, err := os.Stat(p); !os.IsNotExist(err) {
				t.Fatalf("got %v, want %v", err, os.ErrNotExist)
			}
		})
	})
	t.Run("Unlink", func(t *testing.T) {
		t.Run("adds back removed child file", func(t *testing.T) {
			p := filepath.Join(rootdir, "name")
			ok()
			if err := ioutil.WriteFile(p, []byte("Peggy Sue"), 0644); err != nil {
				t.Fatalf("got %v, want nil", err)
			}
			ko()
			if err := os.Remove(p); err == nil {
				t.Fatal("got nil, want non-nil")
			}
			// After remove failure, should still be able to read up the file.
			if b, err := ioutil.ReadFile(p); err != nil {
				t.Errorf("got %v, want nil", err)
			} else if string(b) != "Peggy Sue" {
				t.Errorf("got %q, want %q", b, "Peggy Sue")
			}
		})
	})
}

func testMount(t *testing.T) (mountpoint string, factory *dinoNodeFactory, cleanup func()) {
	t.Helper()

	dir, err := ioutil.TempDir("", "dinofs-test-")
	if err != nil {
		t.Fatal(err)
	}

	factory = &dinoNodeFactory{}
	factory.inogen = newInodeNumbersGenerator()
	go factory.inogen.start()

	factory.metadata = &fakeVersionedStore{}
	factory.blobs = storage.NewBlobStore(storage.NewInMemoryStore())

	var zero [nodeKeyLen]byte
	root := factory.existingNode("root", zero)
	root.mode |= fuse.S_IFDIR
	root.children = make(map[string]*dinoNode)
	factory.root = root

	server, err := fs.Mount(dir, root, &fs.Options{
		UID: uint32(os.Getuid()),
		GID: uint32(os.Getgid()),
	})
	if err != nil {
		factory.inogen.stop()
		t.Fatal(err)
	}

	return dir, factory, func() {
		_ = server.Unmount()
		_ = os.RemoveAll(dir)
		factory.inogen.stop()
	}
}
