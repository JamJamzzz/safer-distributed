// Package storage defines the persistence abstraction SAFER's business
// logic depends on, and the legacy userlib-backed implementation of it.
//
// SAFER-CC (V1) called userlib.Datastore*/Keystore* through a small set of
// wrappers in the client package. SAFER Distributed keeps that funnel but
// turns it into an interface boundary, so that a durable, shared backend
// (MongoDB, Phase 2) can be substituted without touching cryptography,
// authenticated envelopes, UUID addressing, authorization logic, or the
// strict-2PL layer.
//
// The contract deliberately mirrors what SAFER already does: opaque byte
// blobs addressed by the logical uuid.UUIDs SAFER derives itself. This
// layer performs no encryption, no MAC verification, and no authorization;
// all of that stays in the SAFER layer above it, unchanged.
package storage

import (
	"context"

	userlib "github.com/cs161-staff/project2-userlib"
	"github.com/google/uuid"
)

// ObjectStore persists opaque, already-encrypted-and-authenticated SAFER
// objects under the logical UUID SAFER derived for them.
//
// Get reports found=false for an absent id; that is not an error. A
// non-nil error means the backend itself failed (the value of found and
// the returned bytes are then meaningless).
type ObjectStore interface {
	Get(ctx context.Context, id uuid.UUID) (value []byte, found bool, err error)
	Put(ctx context.Context, id uuid.UUID, value []byte) error
	Delete(ctx context.Context, id uuid.UUID) error
}

// KeyStore persists public-key material under SAFER's key names. It holds
// only public keys; private key material never reaches this layer.
//
// Put is write-once per name: re-registering an existing name returns an
// error, matching userlib.KeystoreSet and the SAFER identity semantics
// that depend on it.
type KeyStore interface {
	Get(ctx context.Context, name string) (key userlib.PublicKeyType, found bool, err error)
	Put(ctx context.Context, name string, key userlib.PublicKeyType) error
}

// Atomic is implemented by backends that can make a group of storage
// mutations all-or-nothing.
//
// RunAtomic calls fn with a context derived from ctx. Storage calls made
// with THAT context -- and only those -- take part in the transaction. If
// fn returns an error, every mutation it made is rolled back and the error
// is returned; if it returns nil, they commit together.
//
// The scope is deliberately one context, not one backend and not one
// process. A transaction is reachable only through the context an
// operation passes down its own call stack, so two concurrent operations
// can each be inside their own transaction without any shared mutable
// state to get it wrong.
//
// fn may be called more than once: a backend is allowed to retry it after
// a transient failure. It must therefore be safe to re-run, which in
// practice means computing values before RunAtomic and only writing
// inside it.
type Atomic interface {
	RunAtomic(ctx context.Context, fn func(ctx context.Context) error) error
}

// Storage bundles the halves so a backend can be passed around as one
// value. It is a struct rather than a combined interface because
// ObjectStore.Get and KeyStore.Get take different key types and so cannot
// live on a single Go type.
type Storage struct {
	Objects ObjectStore
	Keys    KeyStore

	// Atomic is optional. A nil Atomic means the backend offers no
	// transactions, and RunAtomic below degrades to running fn directly.
	Atomic Atomic
}

// RunAtomic runs fn atomically when the backend supports it.
//
// When it does not -- the in-memory userlib backend -- fn runs directly on
// ctx. That is not a weaker transaction, it is no transaction: the
// mutations are applied one by one and a failure part-way through leaves
// the earlier ones applied. The userlib backend does not pretend
// otherwise, because a fake transaction that silently commits partial
// state would be worse than an honest absence of one.
func RunAtomic(ctx context.Context, s Storage, fn func(ctx context.Context) error) error {
	if s.Atomic == nil {
		return fn(ctx)
	}
	return s.Atomic.RunAtomic(ctx, fn)
}

// SupportsAtomic reports whether s can make grouped mutations
// all-or-nothing.
func SupportsAtomic(s Storage) bool { return s.Atomic != nil }
