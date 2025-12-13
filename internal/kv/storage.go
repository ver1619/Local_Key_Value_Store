package kv

import (
	"encoding/json"
	"os"
	"sort"
)

// Save writes the store's primary data to disk atomically.
func Save(s *Store, path string) error {
	tmpPath := path + ".tmp"

	s.mu.RLock()
	data, err := json.MarshalIndent(s.data, "", "  ")
	s.mu.RUnlock()
	if err != nil {
		return err
	}

	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}

	return os.Rename(tmpPath, path)
}

// Load loads data from disk and rebuilds the in-memory index.
func Load(s *Store, path string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	var m map[string]string
	if err := json.Unmarshal(b, &m); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.data = m

	// rebuild secondary index
	s.keys = s.keys[:0]
	for k := range m {
		s.keys = append(s.keys, k)
	}
	sort.Strings(s.keys)

	return nil
}
