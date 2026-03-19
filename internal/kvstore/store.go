package kvstore

import (
	"encoding/json"
	"fmt"
	"sync"
)

// Command types for the KV store state machine.
const (
	CmdPut    = "put"
	CmdDelete = "delete"
)

// Command represents a KV operation serialized into a Raft log entry.
type Command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// KVStore is a simple in-memory key-value store backed by a map.
// It serves as the state machine that committed Raft log entries are applied to.
type KVStore struct {
	mu   sync.RWMutex
	data map[string]string
}

// NewKVStore creates a new empty key-value store.
func NewKVStore() *KVStore {
	return &KVStore{
		data: make(map[string]string),
	}
}

// Get retrieves the value for a key. Returns the value and whether the key exists.
func (s *KVStore) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.data[key]
	return val, ok
}

// Put sets a key to a value.
func (s *KVStore) Put(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

// Delete removes a key. No-op if the key does not exist.
func (s *KVStore) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
}

// Apply applies a serialized command from the Raft log to the store.
// Called by the apply loop when a log entry is committed.
func (s *KVStore) Apply(cmdBytes []byte) error {
	var cmd Command
	if err := json.Unmarshal(cmdBytes, &cmd); err != nil {
		return fmt.Errorf("unmarshal command: %w", err)
	}
	switch cmd.Op {
	case CmdPut:
		s.Put(cmd.Key, cmd.Value)
	case CmdDelete:
		s.Delete(cmd.Key)
	default:
		return fmt.Errorf("unknown command op: %s", cmd.Op)
	}
	return nil
}

// SerializePut creates a command byte slice for a put operation.
func SerializePut(key, value string) ([]byte, error) {
	return json.Marshal(Command{Op: CmdPut, Key: key, Value: value})
}

// SerializeDelete creates a command byte slice for a delete operation.
func SerializeDelete(key string) ([]byte, error) {
	return json.Marshal(Command{Op: CmdDelete, Key: key})
}
