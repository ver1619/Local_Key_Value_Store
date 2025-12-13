package kv

import (
	"sort"
	"strings"
	"sync"
)

type Store struct {
	mu   sync.RWMutex
	data map[string]string
	keys []string // sorted keys (secondary index)
}

func NewStore() *Store {
	return &Store{
		data: make(map[string]string),
		keys: make([]string, 0),
	}
}

// Put stores or overwrites the value for the given key.
func (s *Store) Put(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists := s.data[key]
	s.data[key] = value

	if !exists {
		s.keys = append(s.keys, key)
		sort.Strings(s.keys)
	}
}

// Get returns the value for the key.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	v, ok := s.data[key]
	return v, ok
}

// Delete removes the key from the store.
func (s *Store) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.data[key]; !ok {
		return false
	}

	delete(s.data, key)

	for i, k := range s.keys {
		if k == key {
			s.keys = append(s.keys[:i], s.keys[i+1:]...)
			break
		}
	}
	return true
}

func (s *Store) PrefixScan(prefix string) map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string]string)

	for _, k := range s.keys {
		if strings.HasPrefix(k, prefix) {
			result[k] = s.data[k]
		}
		if k > prefix && !strings.HasPrefix(k, prefix) {
			break
		}
	}
	return result
}
