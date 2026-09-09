package mongostore

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"

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
		return nil, fn(sessionCtx)
	}, transactionOptions)
	if err != nil {
		return fmt.Errorf("mongostore: transaction: %w", err)
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
