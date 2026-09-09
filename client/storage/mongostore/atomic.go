package mongostore

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"

	"github.com/JamJamzzz/safer-distributed/client/fencing"
	"github.com/JamJamzzz/safer-distributed/client/storage"
)

var _ storage.Atomic = (*Store)(nil)

// RunAtomic runs fn inside one MongoDB transaction.
//
// Every storage call fn makes with the context it is given joins that
// transaction; calls made with any other context do not. The session
// travels as a context value, so the ordinary per-operation deadline
// wrapping in this package preserves it.
//
// Either every mutation fn made commits, or none does. That is what makes
// a multi-object SAFER mutation -- a file creation writes a chunk,
// metadata, an access box, a status record, a structure record and a
// namespace entry -- all-or-nothing rather than six independent writes
// that can stop half way.
//
// Scope: this is MongoDB's own transaction, not a hand-rolled two-phase
// commit, and it covers exactly one MongoDB deployment. It provides atomic
// durable persistence and nothing else. Locking, authorization, and
// encryption remain SAFER's, above this layer.
//
// fn may run more than once. WithTransaction retries the callback after a
// transient transaction error, so callers must compute their values before
// calling and only write inside fn.
//
// Requires a replica set (or sharded cluster). A standalone mongod cannot
// serve transactions, and reports that plainly rather than silently
// writing without one.
func (s *Store) RunAtomic(ctx context.Context, fn func(ctx context.Context) error) error {
	if fn == nil {
		return fmt.Errorf("mongostore: RunAtomic requires a function")
	}
	// Nesting would silently widen an inner transaction's scope to the
	// outer one, so it is refused rather than quietly flattened.
	if mongo.SessionFromContext(ctx) != nil {
		return fmt.Errorf("mongostore: RunAtomic called inside an existing transaction")
	}

	// Liveness: one explicit deadline for the whole transaction when the
	// caller supplied none.
	//
	// Phase 3B deliberately added no per-statement timeouts inside a
	// transaction, because expiring one statement aborts the whole
	// transaction rather than just that statement, and normal contention
	// would then look like failure. That reasoning still holds. But it
	// left no bound at all on a transaction whose caller passed a
	// context that never ends: a wedged MongoDB operation would keep the
	// transaction -- and, once leases exist, the lease renewals that
	// depend on the operation finishing -- alive indefinitely.
	//
	// A single transaction-level deadline fixes that without
	// reintroducing per-statement timeouts. A caller with its own
	// deadline keeps it; this only fills the gap when there is none.
	if _, hasDeadline := ctx.Deadline(); !hasDeadline && s.transactionTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.transactionTimeout)
		defer cancel()
	}

	session, err := s.client.StartSession()
	if err != nil {
		return fmt.Errorf("mongostore: starting session: %w", err)
	}
	defer session.EndSession(ctx)

	// Majority read and write concern, and snapshot reads: a committed
	// transaction is acknowledged by a majority before it is reported
	// committed, and reads inside it see one consistent point in time.
	transactionOptions := options.Transaction().
		SetReadConcern(readconcern.Snapshot()).
		SetWriteConcern(writeconcern.New(writeconcern.WMajority()))

	_, err = session.WithTransaction(ctx, func(sessionCtx mongo.SessionContext) (interface{}, error) {
		if err := fn(sessionCtx); err != nil {
			return nil, err
		}
		// Fencing is validated last, after the mutation's writes and
		// immediately before commit, so the window between "still hold
		// the lock" and "committed" is as small as the database can make
		// it.
		return nil, s.validateFences(sessionCtx, ctx)
	}, transactionOptions)
	if err != nil {
		return fmt.Errorf("mongostore: transaction: %w", err)
	}
	return nil
}

// validateFences proves, inside the caller's transaction, that every
// fencing grant the operation is relying on is still current.
//
// This is a conditional WRITE, not a read, and that is the whole point.
// The surrounding transaction uses snapshot reads, so reading the fence
// document would return the value as of the snapshot: a writer that was
// revoked after its transaction started would still see its own token and
// happily commit. Updating the document instead puts this transaction into
// direct write conflict with the coordinator's allocation and
// invalidation writes, so the database itself refuses to commit a stale
// writer.
//
// The filter is the full identity of the grant -- resource, exact token,
// and owning transaction -- so it fails if the token advanced (someone
// else was granted the lock) or if ownership was cleared (this
// transaction was revoked or ended).
//
// grants come from the operation's own context, never from global state,
// so concurrent operations validate their own grants and only their own.
func (s *Store) validateFences(sessionCtx mongo.SessionContext, operationCtx context.Context) error {
	if fencing.ValidationSkippedForTest(operationCtx) {
		return nil
	}
	grants := fencing.GrantsFromContext(operationCtx)
	if len(grants) == 0 {
		// Nothing to prove. Operations that take no exclusive lock --
		// and therefore mutate nothing another writer could be fenced
		// against -- legitimately have no grants.
		return nil
	}

	fences := s.client.Database(s.database).Collection(fencing.FencesCollection)
	for _, grant := range grants {
		result, err := fences.UpdateOne(
			sessionCtx,
			bson.M{
				"_id":                 grant.Resource,
				fencing.FieldToken:    int64(grant.Token),
				fencing.FieldOwnerTxn: grant.OwnerTxn,
			},
			bson.M{"$set": bson.M{fencing.FieldValidatedAt: time.Now().UTC()}},
		)
		if err != nil {
			return fmt.Errorf("mongostore: validating %s: %w", grant, err)
		}
		if result.MatchedCount == 0 {
			// The lock moved on without this transaction. Aborting is
			// the correct outcome: its writes were computed against
			// state it no longer owns.
			return fmt.Errorf("mongostore: %s: %w", grant, fencing.ErrStaleFence)
		}
	}
	return nil
}

// Storage returns the bundle SAFER's client layer installs, including this
// backend's transaction capability.
func (s *Store) Storage() storage.Storage {
	return storage.Storage{Objects: s.objects, Keys: s.keys, Atomic: s}
}

// SupportsTransactions reports whether the connected deployment can
// actually serve transactions, by starting one and rolling it back.
//
// A standalone mongod accepts writes happily but rejects transactions, so
// a deployment that cannot do this would silently lose atomicity. Tests
// use it to skip with an accurate reason instead of failing obscurely.
func (s *Store) SupportsTransactions(ctx context.Context) error {
	return s.RunAtomic(ctx, func(context.Context) error { return nil })
}
