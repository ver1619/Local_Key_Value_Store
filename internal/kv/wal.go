package kv

import (
	"bufio"
	"encoding/json"
	"os"
)

const (
	opPut    = "PUT"
	opDelete = "DELETE"
)

type walEntry struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

type WAL struct {
	file *os.File
	w    *bufio.Writer
}

// OpenWAL opens (or creates) a WAL file in append mode.
func OpenWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{
		file: f,
		w:    bufio.NewWriter(f),
	}, nil
}

// AppendPut logs a PUT operation.
func (w *WAL) AppendPut(key, value string) error {
	e := walEntry{Op: opPut, Key: key, Value: value}
	return w.append(e)
}

// AppendDelete logs a DELETE operation.
func (w *WAL) AppendDelete(key string) error {
	e := walEntry{Op: opDelete, Key: key}
	return w.append(e)
}

func (w *WAL) append(e walEntry) error {
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if _, err := w.w.Write(append(b, '\n')); err != nil {
		return err
	}
	if err := w.w.Flush(); err != nil {
		return err
	}
	return w.file.Sync()
}

// Close closes the WAL file.
func (w *WAL) Close() error {
	return w.file.Close()
}

// Replay replays WAL entries into the store.
func ReplayWAL(store *Store, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var e walEntry
		if err := json.Unmarshal(scanner.Bytes(), &e); err != nil {
			return err
		}

		switch e.Op {
		case opPut:
			store.Put(e.Key, e.Value)
		case opDelete:
			store.Delete(e.Key)
		}
	}
	return scanner.Err()
}
