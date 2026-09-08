package storage

import (
	"context"
	"sync"

	userlib "github.com/cs161-staff/project2-userlib"
	"github.com/google/uuid"
)

// ---------------------------------------------------------------------
// Storage-layer thread safety (userlib backend ONLY).
//
// This is the V1 latching discipline, moved verbatim from client.go and
// scoped to the implementation it actually belongs to.
//
// userlib's Datastore/Keystore are plain, unsynchronized Go maps (see
// project2-userlib's datastoreType/keystoreType). They were never designed
// for concurrent access from multiple goroutines -- and Go's runtime
// actively crashes the whole process ("fatal error: concurrent map read
// and map write") the moment two goroutines touch a map concurrently,
// independent of the race detector and independent of which logical keys
// they touch.
//
// This is a distinct concern from saferLockManager's strict 2PL: the
// LockManager coordinates SAFER's own logical resources (namespace
// entries, logical files) so that, e.g., two AppendToFile calls on
// DIFFERENT files never block each other. But even two operations on
// completely unrelated files still both end up calling into the very same
// underlying Go map, and that raw map access itself is not safe without
// its own, separate guard -- analogous to a real database engine's
// storage-layer buffer-pool latches being a different mechanism from its
// transaction manager's row/table locks. datastoreMu/keystoreMu below are
// that guard. DatastoreGet is NOT read-only: userlib v0.5.1 increments its
// shared bandwidth counter even when reading distinct keys, so allowing
// concurrent Gets under RLock races on that counter. The datastore latch
// is therefore held exclusively for the single raw call (including its
// copy and accounting), never for the surrounding logical operation. These
// latches have no TxnID, no 2PL semantics, and no participation in
// saferLockManager whatsoever.
//
// They are package-level rather than per-UserlibStore because the maps
// they protect are themselves process-global inside userlib: two
// UserlibStore values address the same maps and must share one latch.
//
// SAFER Distributed note: this discipline is a property of the userlib
// backend, not of SAFER. It must NOT be imposed on the MongoDB backend,
// whose driver is concurrency-safe and whose whole purpose is concurrent
// shared access. Nothing outside this file may take these latches.
var (
	datastoreMu sync.Mutex
	keystoreMu  sync.RWMutex
)

// UserlibObjectStore and UserlibKeyStore are the legacy, process-local,
// non-durable backend: the in-memory userlib Datastore and Keystore, with
// the V1 latches above.
//
// They preserve V1 behavior exactly and remain the default, so the
// inherited SAFER-CC test suite, benchmarks, and semantics are unchanged.
// They provide no durability across restarts and no visibility between
// processes.
//
// Both zero values are ready to use; they hold no state of their own,
// since the state lives in userlib's globals.
type UserlibObjectStore struct{}

// UserlibKeyStore is the userlib-backed KeyStore. See UserlibObjectStore.
type UserlibKeyStore struct{}

// Compile-time confirmation that the legacy backend satisfies the
// abstraction the SAFER layer depends on.
var (
	_ ObjectStore = (*UserlibObjectStore)(nil)
	_ KeyStore    = (*UserlibKeyStore)(nil)
)

// NewUserlibStorage returns the userlib-backed Storage bundle: SAFER-CC's
// V1 behavior, unchanged.
func NewUserlibStorage() Storage {
	return Storage{Objects: &UserlibObjectStore{}, Keys: &UserlibKeyStore{}}
}

// Get returns the stored bytes for id. ctx is accepted for interface
// conformance; userlib's in-memory maps have no cancellable work, so it is
// not consulted. It never returns an error.
func (s *UserlibObjectStore) Get(ctx context.Context, id uuid.UUID) ([]byte, bool, error) {
	datastoreMu.Lock()
	defer datastoreMu.Unlock()
	value, ok := userlib.DatastoreGet(id)
	return value, ok, nil
}

// Put stores value under id, overwriting any previous value. It never
// returns an error.
func (s *UserlibObjectStore) Put(ctx context.Context, id uuid.UUID, value []byte) error {
	datastoreMu.Lock()
	defer datastoreMu.Unlock()
	userlib.DatastoreSet(id, value)
	return nil
}

// Delete removes id. Deleting an absent id is not an error, matching
// userlib.DatastoreDelete.
func (s *UserlibObjectStore) Delete(ctx context.Context, id uuid.UUID) error {
	datastoreMu.Lock()
	defer datastoreMu.Unlock()
	userlib.DatastoreDelete(id)
	return nil
}

// Get returns the public key registered under name. It never returns an
// error.
func (s *UserlibKeyStore) Get(ctx context.Context, name string) (userlib.PublicKeyType, bool, error) {
	keystoreMu.RLock()
	defer keystoreMu.RUnlock()
	key, ok := userlib.KeystoreGet(name)
	return key, ok, nil
}

// Put registers key under name. It returns userlib's error when name is
// already taken, preserving the write-once identity semantics SAFER relies
// on.
func (s *UserlibKeyStore) Put(ctx context.Context, name string, key userlib.PublicKeyType) error {
	keystoreMu.Lock()
	defer keystoreMu.Unlock()
	return userlib.KeystoreSet(name, key)
}
