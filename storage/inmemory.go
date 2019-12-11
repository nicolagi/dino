package storage

import (
	"fmt"
	"sync"
)

// InMemoryStore is a Store implementation powered by a map, to be used for
// testing or caches.
type InMemoryStore struct {
	sync.Mutex
	m map[string][]byte
}

func init() {
	registerBuilder("in-memory", func(*Builder, map[string]interface{}) (store Store, err error) {
		return NewInMemoryStore(), nil
	})
}

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		m: make(map[string][]byte),
	}
}

func (s *InMemoryStore) Put(key, value []byte) (err error) {
	s.Lock()
	s.m[string(key)] = dup(value)
	s.Unlock()
	return nil
}

func (s *InMemoryStore) Get(key []byte) (value []byte, err error) {
	s.Lock()
	value, ok := s.m[string(key)]
	s.Unlock()
	if !ok {
		return nil, fmt.Errorf("%.40q: %w", key, ErrNotFound)
	}
	if value == nil {
		value = []byte{}
	}
	return value, nil
}
