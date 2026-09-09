// Package fencing defines SAFER's fencing tokens: what they are, how they
// travel from the coordinator to a worker's storage transaction, and what
// a stale one means.
//
// A fencing token exists to stop a writer that no longer holds the lock it
// thinks it holds. The dangerous sequence is:
//
//	worker A is granted X on a file, with token 41
//	A stalls (paused, swapped out, GC, a slow disk) and stops renewing
//	A's lease passes; the coordinator revokes and releases A's locks
//	worker B is granted X on the same file, with token 42, and commits
//	A wakes up and tries to commit the work it computed before the pause
//
// Without fencing, A's write lands on top of B's, silently. The token is
// what makes A's write fail instead: B's grant advanced the resource's
// token past A's, and A's commit is conditional on its own token still
// being current.
//
// The check is deliberately a conditional WRITE, not a read. SAFER's
// storage transactions use MongoDB snapshot reads, so a transaction that
// merely read the fence document would see the snapshot it started with --
// token 41, still apparently valid -- no matter what happened afterwards.
// A conditional update of the same document that the coordinator writes
// when it hands out or invalidates a grant creates a real write-conflict
// boundary between the two, so the stale transaction aborts rather than
// committing on stale information. See ValidateFences in the mongostore
// backend.
//
// Only exclusive grants are fenced. A shared holder cannot corrupt
// anything by being stale, and giving readers tokens would make them
// conflict with each other on the fence document for no benefit.
package fencing

import (
	"context"
	"errors"
	"fmt"
)

// FencesCollection is the MongoDB collection holding fence state. It is
// coordination metadata and holds no SAFER object content: the documents
// are resource identifiers, counters, and owner ids.
//
// Document shape:
//
//	{ _id: "<canonical resource id>", token: <int64>, owner_txn: "<uuid>"|null, ... }
const FencesCollection = "lock_fences"

// Field names in a fence document, shared by the coordinator that writes
// them and the worker that validates against them.
const (
	FieldToken       = "token"
	FieldOwnerTxn    = "owner_txn"
	FieldUpdatedAt   = "updated_at"
	FieldValidatedAt = "validated_at"
)

// Token is a per-resource monotonically increasing fencing token. Zero is
// never issued, so a zero token means "no token", which is what a shared
// grant carries.
type Token uint64

// Grant is one exclusive lock grant's fencing evidence: the resource, the
// token issued for it, and the transaction that owns it.
//
// A worker keeps every grant its transaction was issued and presents all
// of them when it commits. An operation that took X on several resources
// -- file creation takes both a namespace and a file -- must prove every
// one of them, since losing any single lock invalidates the whole
// mutation.
type Grant struct {
	// Resource is the canonical resource identifier, produced by
	// ResourceKey so that the coordinator and the worker agree on it
	// exactly.
	Resource string
	Token    Token
	// OwnerTxn is the external (UUID) transaction identity, as a string,
	// so this package need not depend on the coordination layer.
	OwnerTxn string
}

func (g Grant) String() string {
	return fmt.Sprintf("fence{resource=%s token=%d owner=%s}", g.Resource, g.Token, g.OwnerTxn)
}

// ResourceKey builds the canonical fence-document id for a lock resource.
//
// The type is included, not just the key: a namespace resource and a file
// resource could otherwise collide onto one fence document and one of them
// would silently fence the other.
func ResourceKey(resourceType uint8, key string) string {
	return fmt.Sprintf("%d:%s", resourceType, key)
}

// ErrStaleFence reports that a transaction tried to commit while holding a
// fencing token that is no longer current: the lock was revoked or
// re-granted, and this writer is behind. It is distinguishable on purpose
// -- a caller that sees it knows its work was correctly rejected, not that
// storage failed.
var ErrStaleFence = errors.New("fencing: stale fencing token; this transaction no longer holds the lock")

type grantsKey struct{}

// WithGrants attaches a transaction's fencing grants to a context.
//
// This is how grants reach the storage transaction without any global
// state: they ride the same operation-scoped context that already carries
// the MongoDB session (see the Phase 3B design). Two concurrent
// operations therefore each present their own grants, and neither can see
// the other's.
func WithGrants(ctx context.Context, grants []Grant) context.Context {
	if len(grants) == 0 {
		return ctx
	}
	// Copy, so a later mutation of the caller's slice cannot change what
	// an in-flight transaction will validate.
	stored := make([]Grant, len(grants))
	copy(stored, grants)
	return context.WithValue(ctx, grantsKey{}, stored)
}

// GrantsFromContext returns the fencing grants attached to ctx, if any.
func GrantsFromContext(ctx context.Context) []Grant {
	grants, _ := ctx.Value(grantsKey{}).([]Grant)
	return grants
}

type skipValidationKey struct{}

// WithoutValidationForTest returns a context whose storage transaction
// will NOT validate fencing tokens.
//
// It exists for exactly one purpose: the negative control that proves
// fence validation is what prevents a stale writer from committing. A test
// that bypasses the check must be able to show the bad commit actually
// happens without it, otherwise the fencing tests could be passing for
// unrelated reasons.
//
// Nothing in production calls this, and it is scoped to a single context
// rather than a process-wide switch, so it cannot leak into a concurrent
// operation.
func WithoutValidationForTest(ctx context.Context) context.Context {
	return context.WithValue(ctx, skipValidationKey{}, true)
}

// ValidationSkippedForTest reports whether validation was disabled on this
// context.
func ValidationSkippedForTest(ctx context.Context) bool {
	skipped, _ := ctx.Value(skipValidationKey{}).(bool)
	return skipped
}
